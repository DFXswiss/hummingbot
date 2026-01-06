import asyncio
import decimal
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from async_timeout import timeout
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_utils, xt_web_utils as web_utils
from hummingbot.connector.exchange.xt.xt_api_order_book_data_source import XtAPIOrderBookDataSource
from hummingbot.connector.exchange.xt.xt_api_user_stream_data_source import XtAPIUserStreamDataSource
from hummingbot.connector.exchange.xt.xt_auth import XtAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None


class XtExchange(ExchangePyBase):
    # XT depends on REST API to get trade fills
    # keeping short/long poll intervals 1 second to poll regularly for trade updates.
    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 12.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 xt_api_key: str,
                 xt_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = xt_api_key
        self.secret_key = xt_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(client_config_map)
        
        # Initialize timer for periodic tracked orders logging
        self._last_tracked_orders_log_time = 0
        self._tracked_orders_log_interval = 300  # 5 minutes in seconds
        
        # Track latest polled time for fills (similar to Injective V2)
        self._latest_polled_order_fill_time = 0

    @staticmethod
    def xt_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(xt_type: str) -> OrderType:
        return OrderType[xt_type]

    @property
    def authenticator(self):
        return XtAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        if self._domain == "com":
            return "xt"
        else:
            return f"xt_{self._domain}"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    def supports_batch_order_create(self) -> bool:
        """
        Indicates that XT exchange supports native batch order creation.
        """
        return True

    def batch_order_create(self, orders_to_create: List[Union[MarketOrder, LimitOrder]]) -> List[Union[MarketOrder, LimitOrder]]:
        """
        Issues a batch order creation as a single API request.
        :param orders_to_create: A list of LimitOrder or MarketOrder objects representing the orders to create.
        :returns: A list of LimitOrder or MarketOrder objects representing the created orders with generated order IDs.
        """
        orders_with_ids_to_create = []
        for order in orders_to_create:
            client_order_id = get_new_client_order_id(
                is_buy=order.is_buy,
                trading_pair=order.trading_pair,
                hbot_order_id_prefix=self.client_order_id_prefix,
                max_id_len=self.client_order_id_max_length,
            )
            orders_with_ids_to_create.append(order.copy_with_id(client_order_id=client_order_id))
        
        # Schedule async batch creation
        from hummingbot.core.utils.async_utils import safe_ensure_future
        safe_ensure_future(self._execute_batch_order_create(orders_to_create=orders_with_ids_to_create))
        return orders_with_ids_to_create

    def batch_order_cancel(self, orders_to_cancel: List[LimitOrder]):
        """
        Issues a batch order cancellation as a single API request.
        :param orders_to_cancel: A list of the orders to cancel.
        """
        from hummingbot.core.utils.async_utils import safe_ensure_future
        safe_ensure_future(coro=self._execute_batch_cancel(orders_to_cancel=orders_to_cancel))

    async def _execute_batch_order_create(self, orders_to_create: List[Union[MarketOrder, LimitOrder]]):
        """
        Executes batch order creation using XT's batch endpoint directly.
        Automatically chunks orders into groups of maximum 20.
        """
        if not orders_to_create:
            return
        
        # Split into chunks of 20 (XT API limit)
        chunk_size = 20
        order_chunks = [orders_to_create[i:i + chunk_size] for i in range(0, len(orders_to_create), chunk_size)]
        
        self.logger().info(
            f"[BATCH ORDER CREATE] Creating {len(orders_to_create)} orders in {len(order_chunks)} batch(es)"
        )
        
        for chunk_idx, chunk in enumerate(order_chunks):
            try:
                self.logger().info(
                    f"[BATCH ORDER CREATE] Processing batch {chunk_idx + 1}/{len(order_chunks)} with {len(chunk)} orders"
                )
                
                # Prepare batch items
                items = []
                for order in chunk:
                    order_type = OrderType.LIMIT if isinstance(order, LimitOrder) else OrderType.MARKET
                    price = order.price if order_type == OrderType.LIMIT else Decimal("0")
                    size = order.quantity if order_type == OrderType.LIMIT else order.amount
                    trade_type = TradeType.BUY if order.is_buy else TradeType.SELL
                    
                    amount_str = f"{size:f}"
                    price_str = f"{price:f}"
                    type_str = self.xt_order_type(order_type)
                    side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
                    symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
                    
                    api_params = {
                        "symbol": symbol,
                        "side": side_str,
                        "quantity": amount_str,
                        "type": type_str,
                        "clientOrderId": order.client_order_id,
                        "bizType": "SPOT",
                    }
                    
                    if order_type == OrderType.LIMIT:
                        api_params["price"] = price_str
                        api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC
                    
                    items.append(api_params)
                    
                    # Start tracking the order
                    self.start_tracking_order(
                        order_id=order.client_order_id,
                        exchange_order_id=None,
                        trading_pair=order.trading_pair,
                        order_type=order_type,
                        trade_type=trade_type,
                        price=price,
                        amount=size,
                    )
                
                # Make single batch API call for this chunk
                self.logger().info(f"[BATCH ORDER CREATE] API call for {len(items)} orders")
                result = await self._api_post(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    data={"items": items},
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL
                )
                
                # Process results
                transact_time = self.current_timestamp
                if "result" in result and result["result"] is not None:
                    batch_response = result["result"]
                    results_list = batch_response.get("items", []) if isinstance(batch_response, dict) else batch_response
                    
                    # Create mapping by clientOrderId
                    result_map = {r["clientOrderId"]: r for r in results_list if r and "clientOrderId" in r}
                    
                    # Update order tracking
                    for order in chunk:
                        client_order_id = order.client_order_id
                        if client_order_id in result_map:
                            order_result = result_map[client_order_id]
                            if "orderId" in order_result:
                                exchange_order_id = str(order_result["orderId"])
                                order_update = OrderUpdate(
                                    client_order_id=client_order_id,
                                    exchange_order_id=exchange_order_id,
                                    trading_pair=order.trading_pair,
                                    update_timestamp=transact_time,
                                    new_state=OrderState.OPEN,
                                )
                                self._order_tracker.process_order_update(order_update)
                            else:
                                self.logger().error(f"Order {client_order_id} missing orderId in batch response")
                                self._update_order_after_failure(client_order_id, order.trading_pair)
                        else:
                            self.logger().error(f"Order {client_order_id} not found in batch response")
                            self._update_order_after_failure(client_order_id, order.trading_pair)
                else:
                    self.logger().error(f"Batch order create failed. API response: {result}")
                    for order in chunk:
                        self._update_order_after_failure(order.client_order_id, order.trading_pair)
            
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.logger().error(f"Batch order create exception: {ex}", exc_info=True)
                for order in chunk:
                    self._update_order_after_failure(order.client_order_id, order.trading_pair)

    def _update_order_after_failure(self, order_id: str, trading_pair: str):
        """Helper to mark order as failed"""
        order_update = OrderUpdate(
            client_order_id=order_id,
            trading_pair=trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.FAILED,
        )
        self._order_tracker.process_order_update(order_update)

    async def _execute_batch_cancel(self, orders_to_cancel: List[LimitOrder]):
        """
        Executes batch order cancellation using XT's batch endpoint directly.
        Automatically chunks orders into groups of maximum 20.
        """
        if not orders_to_cancel:
            return
        
        # Get exchange order IDs
        orders_with_exchange_ids = []
        for order in orders_to_cancel:
            tracked_order = self._order_tracker.all_updatable_orders.get(order.client_order_id)
            if tracked_order is not None and tracked_order.exchange_order_id:
                orders_with_exchange_ids.append((order, tracked_order.exchange_order_id))
        
        if not orders_with_exchange_ids:
            return
        
        # Split into chunks of 20 (XT API limit)
        chunk_size = 20
        order_chunks = [orders_with_exchange_ids[i:i + chunk_size] for i in range(0, len(orders_with_exchange_ids), chunk_size)]
        
        self.logger().info(
            f"[BATCH ORDER CANCEL] Canceling {len(orders_with_exchange_ids)} orders in {len(order_chunks)} batch(es)"
        )
        
        for chunk_idx, chunk in enumerate(order_chunks):
            try:
                order_ids = [int(ex_id) for _, ex_id in chunk]
                
                self.logger().info(
                    f"[BATCH ORDER CANCEL] Processing batch {chunk_idx + 1}/{len(order_chunks)} with {len(order_ids)} orders"
                )
                
                result = await self._api_delete(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    data={"orderIds": order_ids},
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL
                )
                
                # Check if successful
                if result.get("rc") == 0:
                    self.logger().info(f"[BATCH ORDER CANCEL] Success for batch {chunk_idx + 1}")
                    for order, _ in chunk:
                        order_update = OrderUpdate(
                            client_order_id=order.client_order_id,
                            trading_pair=order.trading_pair,
                            update_timestamp=self.current_timestamp,
                            new_state=OrderState.CANCELED,
                        )
                        self._order_tracker.process_order_update(order_update)
                else:
                    self.logger().error(f"Batch cancel failed for batch {chunk_idx + 1}. API response: {result}")
            
            except asyncio.CancelledError:
                raise
            except Exception as ex:
                self.logger().error(f"Batch order cancel exception for batch {chunk_idx + 1}: {ex}", exc_info=True)

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("AUTH_002" in error_description or "AUTH_003" in error_description
                                        or "AUTH_004" in error_description or "AUTH_105" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        """
        Determines if the error from status update indicates the order doesn't exist on the exchange.
        
        Context: This is called per-order (not batch) in base class _update_orders_with_error_handler loop.
        
        For XT exchange, we specifically check for:
        - TimeoutError: from get_exchange_order_id() at line 1016 - order never received exchange_order_id
        - ValueError with "ORDER_005": XT API explicitly says order not found
        - ValueError with "invalid literal for int() with base 10: 'None'": exchange_order_id is string "None"
        
        These errors definitively indicate THIS specific order doesn't exist on the exchange.
        """
        error_message = str(status_update_exception)
        return (
            isinstance(status_update_exception, TimeoutError) or
            (isinstance(status_update_exception, ValueError) and 
             ("ORDER_005" in error_message or 
              "invalid literal for int() with base 10: 'None'" in error_message))
        )

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        """
        Determines if the error from cancellation indicates the order doesn't exist on the exchange.
        
        Context: This is called per-order (not batch) in base class _execute_order_cancel.
        
        For XT exchange, same specific error checks as status update:
        - TimeoutError: order never received exchange_order_id
        - ValueError with "ORDER_005": XT API explicitly says order not found
        - ValueError with "invalid literal for int() with base 10: 'None'": exchange_order_id is string "None"
        
        These mean the order cannot be cancelled on the exchange and should be removed from tracking.
        """
        error_message = str(cancelation_exception)
        return (
            isinstance(cancelation_exception, TimeoutError) or
            (isinstance(cancelation_exception, ValueError) and 
             ("ORDER_005" in error_message or 
              "invalid literal for int() with base 10: 'None'" in error_message))
        )

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return XtAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return XtAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        type_str = self.xt_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        
        self.logger().info(
            f"[SINGLE ORDER CREATE] Placing {side_str} {type_str} order: "
            f"client_order_id={order_id}, trading_pair={trading_pair}, amount={amount_str}, price={price}"
        )
        
        api_params = {
            "symbol": symbol,
            "side": side_str,
            "quantity": amount_str,
            "type": type_str,
            "clientOrderId": order_id,
            "bizType": "SPOT",
        }
        if order_type == OrderType.LIMIT:
            price_str = f"{price:f}"
            api_params["price"] = price_str
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC
        
        order_result = await self._api_post(
            path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True,
            limit_id=CONSTANTS.CREATE_ORDER_PATH_URL
        )
        
        if order_result.get("rc") != 0:
            raise IOError(f"Error submitting order. API response: {order_result}")
        
        result = order_result["result"]
        exchange_order_id = str(result["orderId"])
        self.logger().info(f"[SINGLE ORDER CREATE] Success: exchange_order_id={exchange_order_id}")
        return exchange_order_id, self.current_timestamp

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        self._log_tracked_orders_if_needed()
        ex_order_id = await tracked_order.get_exchange_order_id()
        
        self.logger().info(
            f"[SINGLE ORDER CANCEL] Canceling order: "
            f"client_order_id={order_id}, exchange_order_id={ex_order_id}"
        )
        
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={
                "orderId": int(ex_order_id)
            },
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_PATH_URL
        )
        
        if cancel_result.get("rc") != 0:
            raise IOError(f"Error canceling order {order_id}. API response: {cancel_result}")
        
        self.logger().info(f"[SINGLE ORDER CANCEL] Success: client_order_id={order_id}")


    async def _execute_order_cancel(self, order: InFlightOrder) -> str:
        try:
            await self._place_cancel(order.client_order_id, order)
            return order.client_order_id
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            # Binance does not allow cancels with the client/user order id
            # so log a warning and wait for the creation of the order to complete
            self.logger().warning(
                f"Failed to cancel the order {order.client_order_id} because it does not have an exchange order id yet")
            await self._order_tracker.process_order_not_found(order.client_order_id)
        except Exception:
            self.logger().error(
                f"Failed to cancel order {order.client_order_id}", exc_info=True)

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        """
        Cancels all currently active orders using batch cancellation.
        Chunking is handled automatically by _execute_batch_cancel.
        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run
        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []
        failed_cancellations = []

        if len(incomplete_orders) == 0:
            return []

        try:
            async with timeout(timeout_seconds):
                self.logger().info(f"[CANCEL ALL] Canceling {len(incomplete_orders)} orders via batch")
                
                # Convert to LimitOrder objects for batch_order_cancel
                orders_to_cancel = []
                for order in incomplete_orders:
                    limit_order = LimitOrder(
                        client_order_id=order.client_order_id,
                        trading_pair=order.trading_pair,
                        is_buy=order.trade_type == TradeType.BUY,
                        base_currency=order.trading_pair.split("-")[0],
                        quote_currency=order.trading_pair.split("-")[1],
                        price=order.price,
                        quantity=order.amount
                    )
                    orders_to_cancel.append(limit_order)
                
                # Call batch cancel (chunking handled automatically inside)
                self.batch_order_cancel(orders_to_cancel=orders_to_cancel)
                
                # Give time for the cancelled statuses to be processed
                # KILL_TIMEOUT is 10 seconds default in hummingbot_application
                await self._sleep(2.0)

                open_orders = await self._get_open_orders()
                failed_cancellations = set([o for o in open_orders if o in order_id_set])
                successful_cancellations = order_id_set - failed_cancellations
                successful_cancellations = [CancellationResult(client_order_id, True) for client_order_id in successful_cancellations]
                failed_cancellations = [CancellationResult(client_order_id, False) for client_order_id in failed_cancellations]
                
                self.logger().info(
                    f"[CANCEL ALL] Complete: {len(successful_cancellations)} successful, {len(failed_cancellations)} failed"
                )
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection."
            )
        return successful_cancellations + failed_cancellations

    async def _cancel_lost_orders(self):
        """
        Override to use batch cancellation for lost orders instead of individual calls.
        """
        lost_orders = self._order_tracker.lost_orders.copy()
        if not lost_orders:
            return
        
        self.logger().info(f"[BATCH CANCEL LOST] Canceling {len(lost_orders)} lost orders via batch")
        
        # Separate orders with and without exchange IDs
        orders_with_exchange_ids = []
        orders_without_exchange_ids = []
        
        for order in lost_orders.values():
            # Check for both None and string "None" (legacy data from str(None) bug)
            if order.exchange_order_id and order.exchange_order_id != "None":
                orders_with_exchange_ids.append(order)
            else:
                orders_without_exchange_ids.append(order)
        
        for order in orders_without_exchange_ids:
            self.logger().warning(
                f"Lost order {order.client_order_id} does not have an exchange_order_id. "
                f"Cannot cancel on exchange. Removing from lost orders."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        
        if not orders_with_exchange_ids:
            return
        
        # Process in chunks (max 20 per batch)
        chunk_size = 20
        for i in range(0, len(orders_with_exchange_ids), chunk_size):
            chunk = orders_with_exchange_ids[i:i + chunk_size]
            
            try:
                exchange_order_ids = [int(order.exchange_order_id) for order in chunk]
                
                self.logger().info(
                    f"[BATCH CANCEL LOST] Sending batch cancel for {len(exchange_order_ids)} lost orders"
                )
                
                cancel_result = await self._api_delete(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    data={"orderIds": exchange_order_ids},
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL
                )
                
                if cancel_result.get("rc") == 0:
                    # Batch succeeded, update order states
                    for order in chunk:
                        order_update = OrderUpdate(
                            client_order_id=order.client_order_id,
                            exchange_order_id=order.exchange_order_id,
                            trading_pair=order.trading_pair,
                            update_timestamp=self.current_timestamp,
                            new_state=OrderState.CANCELED,
                        )
                        self._order_tracker.process_order_update(order_update)
                    
                    self.logger().info(
                        f"[BATCH CANCEL LOST] Successfully canceled {len(chunk)} lost orders"
                    )
                else:
                    # Batch failed
                    error_msg = f"Batch cancel failed. API response: {cancel_result}"
                    self.logger().error(f"[BATCH CANCEL LOST ERROR] {error_msg}")
            
            except Exception as e:
                self.logger().error(
                    f"[BATCH CANCEL LOST ERROR] Exception canceling lost orders: {e}",
                    exc_info=True
                )

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Example:
        {
            id: 614,
            symbol: "btc_usdt",
            state: "ONLINE",
            tradingEnabled: true,
            baseCurrency: "btc",
            baseCurrencyPrecision: 10,
            quoteCurrency: "usdt",
            quoteCurrencyPrecision: 8,
            pricePrecision: 2,
            quantityPrecision: 6,
            orderTypes: ["LIMIT", "MARKET"],
            timeInForces: ["GTC", "IOC"],
            filters: [
                {
                    filter: "QUOTE_QTY",
                    min: "1"
                }
            ]
        }
        """
        trading_pair_rules = exchange_info_dict["result"].get("symbols", [])
        retval = []
        for rule in filter(xt_utils.is_exchange_information_valid, trading_pair_rules):

            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))
                filters = rule.get("filters")
                quote_qty_size_filter = next((f for f in filters if f.get("filter") == "QUOTE_QTY"), None)
                quantity_size_filter = next((f for f in filters if f.get("filter") == "QUANTITY"), None)

                min_order_size = Decimal(quantity_size_filter["min"]) if quantity_size_filter is not None \
                    and quantity_size_filter.get("min") is not None else Decimal("0")
                min_notional_size = Decimal(quote_qty_size_filter["min"]) if quote_qty_size_filter is not None \
                    and quote_qty_size_filter.get("min") is not None else Decimal("0")
                min_price_increment = Decimal("1") / (Decimal("10")**Decimal(rule.get("pricePrecision")))
                min_base_amount_increment = Decimal("1") / (Decimal("10")**Decimal(rule.get("quantityPrecision")))

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=min_price_increment,
                                min_base_amount_increment=min_base_amount_increment,
                                min_notional_size=min_notional_size))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _update_trading_fees(self):
        """
        Update fees information from the exchange
        """
        pass

    async def _cancelled_order_handler(self, client_order_id: str, order_update: Optional[Dict[str, Any]]):
        """
        Custom function to handle XT's cancelled orders. Wait until all the trade fills of the order are recorded.
        """
        try:
            executed_amount_base = Decimal(str(order_update.get("eq"))) or Decimal(str(order_update.get("executedQty")))
        except decimal.InvalidOperation:
            executed_amount_base = Decimal("0")

        # if cancelled event comes before we have all the fills of that order,
        # wait 2 cycles to fetch trade fills before updating the status
        for _ in range(2):
            if self.in_flight_orders.get(client_order_id, None) is not None and \
                    self.in_flight_orders.get(client_order_id).executed_amount_base < executed_amount_base:
                await self._sleep(self.LONG_POLL_INTERVAL)
            else:
                break

        if self.in_flight_orders.get(client_order_id, None) is not None and \
                self.in_flight_orders.get(client_order_id).executed_amount_base < executed_amount_base:
            self.logger().warning(
                f"The order fill updates did not arrive on time for {client_order_id}. "
                f"The cancel update will be processed with incomplete information.")

    async def _user_stream_event_listener(self):
        """
        This functions runs in background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("event")
                if event_type == "order":
                    order_update = event_message.get("data")
                    client_order_id = order_update.get("ci")

                    tracked_order = next(
                        (order for order in self._order_tracker.all_updatable_orders.values() if order.client_order_id == client_order_id),
                        None)

                    if tracked_order is not None:

                        if CONSTANTS.ORDER_STATE[order_update.get("st")] == OrderState.CANCELED:
                            await self._cancelled_order_handler(tracked_order.client_order_id, order_update)

                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=(order_update["t"] * 1e-3) if "t" in order_update and order_update["t"] is not None else None,
                            new_state=CONSTANTS.ORDER_STATE[order_update["st"]],
                            client_order_id=tracked_order.client_order_id,
                            exchange_order_id=str(order_update["i"]),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "balance":
                    balance_entry = event_message["data"]
                    asset_name = balance_entry["c"].upper()
                    total_balance = Decimal(balance_entry["b"])
                    frozen_balance = Decimal(balance_entry["f"])
                    free_balance = total_balance - frozen_balance
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def _log_tracked_orders_if_needed(self):
        """Log all tracked order IDs every ~5 minutes"""
        current_time = self.current_timestamp
        if current_time - self._last_tracked_orders_log_time >= self._tracked_orders_log_interval:
            all_orders = list(self.in_flight_orders.values())
            if all_orders:
                order_ids = [f"{o.client_order_id} (ex_id: {o.exchange_order_id}, state: {o.current_state.name})" 
                            for o in all_orders]
                self.logger().info(
                    f"[TRACKED ORDERS] {len(all_orders)} orders being tracked: {', '.join(order_ids)}"
                )
            else:
                self.logger().info("[TRACKED ORDERS] No orders being tracked")
            self._last_tracked_orders_log_time = current_time

    async def _update_orders(self):
        self.logger().info(f"[REST BATCH ORDER] _update_orders called - orders to update: {len(self.in_flight_orders)}")
        
        # Log all tracked orders periodically
        self._log_tracked_orders_if_needed()
        
        orders_to_update = self.in_flight_orders.copy()
        
        # Collect orders with exchange IDs
        orders_with_exchange_ids = []
        orders_without_exchange_ids = []
        
        for order in orders_to_update.values():
            if order.exchange_order_id:
                orders_with_exchange_ids.append(order)
            else:
                orders_without_exchange_ids.append(order)
        
        # Handle orders without exchange IDs (similar to old method's TimeoutError handling)
        for order in orders_without_exchange_ids:
            self.logger().debug(
                f"Tracked order {order.client_order_id} does not have an exchange id. "
                f"Attempting fetch in next polling interval."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        
        if len(orders_with_exchange_ids) == 0:
            return
        
        chunk_size = 20
        order_chunks = [orders_with_exchange_ids[i:i + chunk_size] for i in range(0, len(orders_with_exchange_ids), chunk_size)]
        
        for chunk in order_chunks:
            try:
                exchange_order_ids = ",".join([str(int(order.exchange_order_id)) for order in chunk])
                
                self.logger().info(f"[REST BATCH ORDER STATUS] Fetching batch status - chunk_size: {len(chunk)}, order_ids: {exchange_order_ids}")
                response = await self._api_get(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    params={
                        "orderIds": exchange_order_ids
                    },
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL)
                
                if "result" not in response or response["result"] is None:
                    # Handle as not found for all orders in this chunk
                    for order in chunk:
                        self.logger().network(
                            f"No result returned for order {order.client_order_id}",
                            app_warning_msg=f"Failed to fetch status update for the order {order.client_order_id}.",
                        )
                        await self._order_tracker.process_order_not_found(order.client_order_id)
                    continue

                result_updates = response["result"]
                
                # Track which orders were found in the response
                found_order_ids = set()
                
                for result_update in result_updates:
                    if result_update is not None:
                        client_order_id = result_update["clientOrderId"]
                        found_order_ids.add(client_order_id)
                        
                        if client_order_id in self.in_flight_orders:
                            order_update = OrderUpdate(
                                trading_pair=self.in_flight_orders[client_order_id].trading_pair,
                                update_timestamp=(result_update["updatedTime"] * 1e-3) if "updatedTime" in result_update and result_update["updatedTime"] is not None else None,
                                new_state=CONSTANTS.ORDER_STATE[result_update["state"]],
                                client_order_id=client_order_id,
                                exchange_order_id=str(result_update["orderId"]),
                            )
                            self._order_tracker.process_order_update(order_update)
                
                # Handle orders that weren't found in the response
                for order in chunk:
                    if order.client_order_id not in found_order_ids:
                        self.logger().network(
                            f"Order {order.client_order_id} not found in batch response.",
                            app_warning_msg=f"Failed to fetch status update for the order {order.client_order_id}.",
                        )
                        await self._order_tracker.process_order_not_found(order.client_order_id)
            
            except asyncio.CancelledError:
                # Critical: re-raise to allow proper async cleanup
                raise
            except Exception as e:
                # Handle errors for all orders in this chunk
                self.logger().error(f"Error updating order batch: {e}")
                for order in chunk:
                    self.logger().network(
                        f"Error fetching status update for the order {order.client_order_id}: {e}.",
                        app_warning_msg=f"Failed to fetch status update for the order {order.client_order_id}.",
                    )
                    await self._order_tracker.process_order_not_found(order.client_order_id)
        
    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        """
        Batch fetch fills for all orders by trading pair and timestamp (Injective V2 style).
        """
        if not orders:
            return
        
        # Find oldest order creation time
        oldest_order_creation_time = self.current_timestamp
        for order in orders:
            oldest_order_creation_time = min(oldest_order_creation_time, order.creation_timestamp)
        
        # Group orders by trading pair
        orders_by_trading_pair = {}
        for order in orders:
            if order.trading_pair not in orders_by_trading_pair:
                orders_by_trading_pair[order.trading_pair] = []
            orders_by_trading_pair[order.trading_pair].append(order)
        
        # Determine start time for fetching trades
        start_time = min(oldest_order_creation_time, self._latest_polled_order_fill_time)
        start_time_ms = int(start_time * 1000)
        
        self.logger().info(
            f"[BATCH FILLS UPDATE] Fetching fills for {len(orders)} orders across "
            f"{len(orders_by_trading_pair)} trading pair(s), start_time={start_time}"
        )
        
        # Fetch trades for each trading pair
        for trading_pair, pair_orders in orders_by_trading_pair.items():
            try:
                # Create a set of client order IDs we care about
                target_order_ids = {order.client_order_id for order in pair_orders}
                
                symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                
                self.logger().info(
                    f"[BATCH FILLS UPDATE] Fetching trades for {trading_pair} ({symbol}) "
                    f"with {len(pair_orders)} orders, startTime={start_time_ms}"
                )
                
                # Fetch ALL trades for this trading pair since start_time
                response = await self._api_get(
                    path_url=CONSTANTS.MY_TRADES_PATH_URL,
                    params={
                        "symbol": symbol,
                        "startTime": start_time_ms,
                    },
                    is_auth_required=True,
                    limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
                )
                
                if "result" not in response or response["result"] is None:
                    self.logger().debug(f"No trade results for {trading_pair}")
                    continue
                
                all_fills_response = response["result"]
                trades = all_fills_response.get("items", [])
                
                self.logger().info(
                    f"[BATCH FILLS UPDATE] Received {len(trades)} total trades for {trading_pair}"
                )
                
                # Process trades that belong to our tracked orders
                processed_count = 0
                for trade in trades:
                    # Find the order this trade belongs to
                    client_order_id = trade.get("clientOrderId")
                    
                    # Only process trades for the orders we're tracking
                    if client_order_id not in target_order_ids:
                        continue
                    
                    # Find the order object
                    order = next((o for o in pair_orders if o.client_order_id == client_order_id), None)
                    if order is None:
                        continue
                    
                    exchange_order_id = str(trade["orderId"])
                    fee = TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=order.trade_type,
                        percent_token=trade["feeCurrency"],
                        flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=trade["feeCurrency"])]
                    )
                    
                    fill_timestamp = trade["time"] * 1e-3
                    trade_update = TradeUpdate(
                        trade_id=str(trade["tradeId"]),
                        client_order_id=client_order_id,
                        exchange_order_id=exchange_order_id,
                        trading_pair=trading_pair,
                        fee=fee,
                        fill_base_amount=Decimal(trade["quantity"]),
                        fill_quote_amount=Decimal(trade["quoteQty"]),
                        fill_price=Decimal(trade["price"]),
                        fill_timestamp=fill_timestamp,
                    )
                    
                    self._order_tracker.process_trade_update(trade_update)
                    
                    # Update latest polled time
                    self._latest_polled_order_fill_time = max(
                        self._latest_polled_order_fill_time,
                        fill_timestamp
                    )
                    processed_count += 1
                
                self.logger().info(
                    f"[BATCH FILLS UPDATE] Processed {processed_count} fills for tracked orders in {trading_pair}"
                )
                
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates for {trading_pair}. Error: {request_error}",
                    exc_info=request_error,
                )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        """
        DEPRECATED: This method is no longer used since we override _update_orders_fills.
        Kept for compatibility but returns empty list.
        """
        return []

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        #return await self._batch_order_status_handler.request_order_status(tracked_order) ## TODO: REMOVE ENTIRELY, THE BASE CLASS SUPPORTING THIS SHOULD BE REMOVED TOO.
        client_order_id = tracked_order.client_order_id
        exchange_order_id = await tracked_order.get_exchange_order_id()
        self.logger().info(f"[REST ORDER STATUS] _request_order_status called - client_order_id: {client_order_id}, exchange_order_id: {exchange_order_id}")
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={
                "orderId": int(exchange_order_id),
                "clientOrderId": client_order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_PATH_URL)

        # Check if XT explicitly says order not found
        if response.get("mc") == "ORDER_005":
            # ORDER_005 = order not found, definitively doesn't exist on exchange
            raise ValueError(f"Order {client_order_id} not found on exchange (XT error code: ORDER_005)")
        
        # order update might've already come through user stream listner
        # and order might no longer be available on the exchange.
        if "result" not in response or response["result"] is None:
            # Return OrderUpdate with current state preserved (like Cube exchange)
            # Don't assume what happened - maybe filled via user stream, maybe API issue
            self.logger().debug(
                f"Order {client_order_id} not in API response. Preserving current state. Response: {response}"
            )
            return OrderUpdate(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=tracked_order.trading_pair,
                update_timestamp=self.current_timestamp,
                new_state=tracked_order.current_state,
            )

        updated_order_data = response["result"]
        new_state = CONSTANTS.ORDER_STATE[updated_order_data["state"]]

        if new_state == OrderState.CANCELED:
            await self._cancelled_order_handler(client_order_id, updated_order_data)

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["orderId"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=(updated_order_data["updatedTime"] * 1e-3) if "updatedTime" in updated_order_data and updated_order_data["updatedTime"] is not None else None,
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        self.logger().info(f"[REST BALANCES] _update_balances called")
        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.GLOBAL_RATE_LIMIT)


        if "result" not in account_info or account_info["result"] is None:
            raise IOError(f"Error fetching account updates. API response: {account_info}")

        balances = account_info["result"]["assets"]
        for balance_entry in balances:
            asset_name = balance_entry["currency"].upper()
            free_balance = Decimal(balance_entry["availableAmount"])
            total_balance = Decimal(balance_entry["availableAmount"]) + Decimal(balance_entry["frozenAmount"])
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        for symbol_data in filter(xt_utils.is_exchange_information_valid, exchange_info["result"]["symbols"]):
            mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["baseCurrency"].upper(),
                                                                        quote=symbol_data["quoteCurrency"].upper())
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        self.logger().info(f"[REST TICKER] _get_last_traded_price called - trading_pair: {trading_pair}, symbol: {params['symbol']}")
        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params,
            limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
        )

        return float(resp_json["result"]["p"])

    async def _get_open_orders(self):
        """
        Get all pending orders for the current spot trading pair.
        """
        self.logger().info(f"[REST OPEN ORDERS] _get_open_orders called - trading_pairs: {len(self._trading_pairs)}")
        tasks = []
        for trading_pair in self._trading_pairs:

            params = {
                "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
                "bizType": "SPOT"
            }

            task = self._api_get(
                path_url=CONSTANTS.OPEN_ORDER_PATH_URL,
                params=params,
                is_auth_required=True,
                limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
            )

            tasks.append(task)

        open_orders = []
        responses = await safe_gather(*tasks, return_exceptions=True)
        for response in responses:
            if not isinstance(response, Exception) and "result" in response and isinstance(response["result"], list):
                for order in response["result"]:
                    open_orders.append(order["clientOrderId"])

        return open_orders

    async def _make_network_check_request(self):
        """Override to use global rate limit for network check requests."""
        self.logger().info(f"[REST NETWORK CHECK] _make_network_check_request called")
        await self._api_get(
            path_url=self.check_network_request_path,
            limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
        )

    async def _make_trading_rules_request(self) -> Any:
        """Override to use global rate limit for trading rules requests."""
        self.logger().info(f"[REST TRADING RULES] _make_trading_rules_request called")
        exchange_info = await self._api_get(
            path_url=self.trading_rules_request_path,
            limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
        )
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        """Override to use global rate limit for trading pairs requests."""
        self.logger().info(f"[REST TRADING PAIRS] _make_trading_pairs_request called")
        exchange_info = await self._api_get(
            path_url=self.trading_pairs_request_path,
            limit_id=CONSTANTS.GLOBAL_RATE_LIMIT
        )
        return exchange_info
