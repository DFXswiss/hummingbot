import asyncio
import decimal
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from async_timeout import timeout
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS, xt_utils, xt_web_utils as web_utils
from hummingbot.connector.exchange.xt.xt_api_order_book_data_source import XtAPIOrderBookDataSource
from hummingbot.connector.exchange.xt.xt_api_user_stream_data_source import XtAPIUserStreamDataSource
from hummingbot.connector.exchange.xt.xt_auth import XtAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None


class XtExchange(ExchangePyBase):
    # Optimized polling intervals - WebSocket handles real-time updates
    # REST polling is now just a fallback for orders without WebSocket updates
    SHORT_POLL_INTERVAL = 5.0   # Reduced from 1.0 - for active orders
    LONG_POLL_INTERVAL = 10.0   # Reduced from 1.0 - for general status checks

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
        
        # WebSocket optimization: Track order updates and fill fetches
        self._ws_order_updates = {}  # {client_order_id: timestamp}
        self._ws_order_fills_triggered = set()  # Prevent duplicate fill fetches
        self._performance_metrics = {
            "ws_updates_received": 0,
            "rest_polls_executed": 0,
            "rest_polls_skipped": 0,
            "fill_fetches_triggered": 0,
            "last_reset": 0
        }
        
        super().__init__(client_config_map)

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

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("AUTH_002" in error_description or "AUTH_003" in error_description
                                        or "AUTH_004" in error_description or "AUTH_105" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        pass

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        pass

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
        order_result = None
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        type_str = XtExchange.xt_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {"symbol": symbol,
                      "side": side_str,
                      "quantity": amount_str,
                      "type": type_str,
                      "clientOrderId": order_id,
                      "bizType": "SPOT",
                      "price": price_str}
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC

        order_result = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=api_params,
            is_auth_required=True)

        if "result" not in order_result or order_result["result"] is None:
            raise IOError(f"Error submitting order to XT. API response: {order_result}")

        o_id = str(order_result["result"]["orderId"])
        transact_time = self.current_timestamp

        await asyncio.sleep(0.2)

        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        ex_order_id = await tracked_order.get_exchange_order_id()
        api_params = {
            "orderId": ex_order_id,
        }
        await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True,
            limit_id=CONSTANTS.MANAGE_ORDER)

        await asyncio.sleep(0.2)

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
        Cancels all currently active orders. The cancellations are performed in parallel tasks.
        :param timeout_seconds: the maximum time (in seconds) the cancel logic should run
        :return: a list of CancellationResult instances, one for each of the orders to be cancelled
        """
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        tasks = [self._execute_cancel(o.trading_pair, o.client_order_id) for o in incomplete_orders]
        order_id_set = set([o.client_order_id for o in incomplete_orders])
        successful_cancellations = []
        failed_cancellations = []

        try:
            async with timeout(timeout_seconds):
                await safe_gather(*tasks, return_exceptions=True)

                # Give time for the cancelled statuses to be processed
                # KILL_TIMEOUT is 10 seconds default in hummingbot_application
                await self._sleep(1.0)

                open_orders = await self._get_open_orders()
                failed_cancellations = set([o for o in open_orders if o in order_id_set])
                successful_cancellations = order_id_set - failed_cancellations
                successful_cancellations = [CancellationResult(client_order_id, True) for client_order_id in successful_cancellations]
                failed_cancellations = [CancellationResult(client_order_id, False) for client_order_id in failed_cancellations]
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection."
            )
        return successful_cancellations + failed_cancellations

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
                    
                    # Track WebSocket order update
                    if client_order_id:
                        self._ws_order_updates[client_order_id] = self.current_timestamp
                        self._performance_metrics["ws_updates_received"] += 1
                        self.logger().debug(
                            f"WS order update for {client_order_id}: {order_update.get('st')} "
                            f"(Total WS updates: {self._performance_metrics['ws_updates_received']})"
                        )

                    tracked_order = next(
                        (order for order in self._order_tracker.all_updatable_orders.values() if order.client_order_id == client_order_id),
                        None)

                    if tracked_order is not None:
                        new_state = CONSTANTS.ORDER_STATE[order_update.get("st")]
                        
                        # Trigger fill fetch for FILLED or PARTIALLY_FILLED orders
                        if new_state in [OrderState.FILLED, OrderState.PARTIALLY_FILLED]:
                            fill_key = f"{client_order_id}_{new_state}"
                            if fill_key not in self._ws_order_fills_triggered:
                                self._ws_order_fills_triggered.add(fill_key)
                                self._performance_metrics["fill_fetches_triggered"] += 1
                                self.logger().info(
                                    f"Triggering fill fetch for {client_order_id} with state {new_state}"
                                )
                                safe_ensure_future(self._fetch_fills_for_order(tracked_order))

                        if new_state == OrderState.CANCELED:
                            await self._cancelled_order_handler(tracked_order.client_order_id, order_update)

                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=(order_update["t"] * 1e-3) if "t" in order_update and order_update["t"] is not None else None,
                            new_state=new_state,
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

    async def _update_orders(self):
        """
        Optimized: Only poll orders without recent WebSocket updates
        """
        current_time = self.current_timestamp
        all_orders = self.in_flight_orders.copy()
        orders_to_poll = {}
        
        # Stats for monitoring
        ws_updated = 0
        needs_polling = 0
        
        # Clean up old WebSocket update records (older than 5 minutes)
        self._ws_order_updates = {
            oid: timestamp 
            for oid, timestamp in self._ws_order_updates.items()
            if current_time - timestamp < 300
        }
        
        for client_order_id, order in all_orders.items():
            last_ws_update = self._ws_order_updates.get(client_order_id, 0)
            time_since_update = current_time - last_ws_update
            
            if last_ws_update == 0:
                # Never received a WebSocket update for this order
                orders_to_poll[client_order_id] = order
                needs_polling += 1
                self.logger().debug(f"{client_order_id}: No WS update ever received")
            elif time_since_update > 30:
                # WebSocket update is too old (> 30 seconds)
                orders_to_poll[client_order_id] = order
                needs_polling += 1
                self.logger().debug(f"{client_order_id}: WS update {time_since_update:.1f}s old")
            else:
                # Fresh WebSocket update available, skip REST polling
                ws_updated += 1
                self._performance_metrics["rest_polls_skipped"] += 1
        
        # Log performance metrics
        if len(all_orders) > 0:
            self.logger().info(
                f"Order update stats: {ws_updated} via WS (skipped), {needs_polling} need polling "
                f"(Total orders: {len(all_orders)})"
            )
        
        # Only poll orders without recent WebSocket updates
        for client_order_id, order in orders_to_poll.items():
            try:
                self._performance_metrics["rest_polls_executed"] += 1
                order_update = await self._request_order_status(tracked_order=order)
                if client_order_id in self.in_flight_orders and order_update is not None:
                    self._order_tracker.process_order_update(order_update)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                self.logger().debug(
                    f"Tracked order {client_order_id} does not have an exchange id. "
                    f"Attempting fetch in next polling interval."
                )
                await self._order_tracker.process_order_not_found(client_order_id)
            except Exception as request_error:
                self.logger().network(
                    f"Error fetching status update for the order {order.client_order_id}: {request_error}.",
                    app_warning_msg=f"Failed to fetch status update for the order {order.client_order_id}.",
                )
                await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _fetch_fills_for_order(self, order: InFlightOrder):
        """
        Event-triggered trade fill fetching for FILLED/PARTIALLY_FILLED orders.
        Called from WebSocket handler to avoid continuous polling.
        """
        try:
            # Prevent duplicate fetch attempts
            if hasattr(order, '_fetching_fills') and order._fetching_fills:
                self.logger().debug(f"Already fetching fills for {order.client_order_id}, skipping")
                return
            
            order._fetching_fills = True
            self.logger().debug(f"Fetching trade fills for {order.client_order_id}")
            
            trade_updates = await self._all_trade_updates_for_order(order)
            
            if trade_updates:
                self.logger().info(
                    f"Got {len(trade_updates)} trade fills for {order.client_order_id}"
                )
                for trade_update in trade_updates:
                    self._order_tracker.process_trade_update(trade_update)
            else:
                self.logger().debug(f"No trade fills found for {order.client_order_id}")
                
        except Exception as e:
            self.logger().error(
                f"Failed to fetch trade fills for {order.client_order_id}: {e}",
                exc_info=True
            )
        finally:
            order._fetching_fills = False
    
    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []

        # Do not fetch trades for cached orders(canceled orders). We are using XT Rest API to process trade fills
        # and custom XT order cancel handling(in order status update and user stream listner loop) will take care of cancelled order fills.
        if order.client_order_id not in self._order_tracker.cached_orders:
            exchange_order_id = await order.get_exchange_order_id()
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            response = await self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={
                    "symbol": trading_pair,
                    "orderId": int(exchange_order_id)
                },
                is_auth_required=True,
                limit_id=CONSTANTS.MANAGE_ORDER)
            await asyncio.sleep(0.2)

            # order update might've already come through user stream listner
            # and order might no longer be available on the exchange.
            if "result" not in response or response["result"] is None:
                return trade_updates

            all_fills_response = response["result"]
            for trade in all_fills_response["items"]:
                exchange_order_id = str(trade["orderId"])
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=trade["feeCurrency"],
                    flat_fees=[TokenAmount(amount=Decimal(trade["fee"]), token=trade["feeCurrency"])]
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["tradeId"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["quantity"]),
                    fill_quote_amount=Decimal(trade["quoteQty"]),
                    fill_price=Decimal(trade["price"]),
                    fill_timestamp=trade["time"] * 1e-3,
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        client_order_id = tracked_order.client_order_id
        exchange_order_id = await tracked_order.get_exchange_order_id()
        response = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={
                "orderId": int(exchange_order_id),
                "clientOrderId": client_order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.MANAGE_ORDER)
        await asyncio.sleep(0.2)

        # order update might've already come through user stream listner
        # and order might no longer be available on the exchange.
        if "result" not in response or response["result"] is None:
            return

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

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True)

        await asyncio.sleep(0.2)

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

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        return float(resp_json["result"]["p"])

    async def _get_open_orders(self):
        """
        Get all pending orders for the current spot trading pair.
        """
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
                limit_id=CONSTANTS.MANAGE_ORDER
            )

            tasks.append(task)

        open_orders = []
        responses = await safe_gather(*tasks, return_exceptions=True)
        for response in responses:
            if not isinstance(response, Exception) and "result" in response and isinstance(response["result"], list):
                for order in response["result"]:
                    open_orders.append(order["clientOrderId"])

        return open_orders
    
    async def _log_performance_metrics(self):
        """
        Periodically log performance metrics to monitor WebSocket optimization effectiveness.
        This helps track the reduction in REST API calls and overall performance improvement.
        """
        while True:
            await self._sleep(60)  # Log metrics every 60 seconds
            
            if self._performance_metrics["last_reset"] == 0:
                self._performance_metrics["last_reset"] = self.current_timestamp
            
            time_since_reset = self.current_timestamp - self._performance_metrics["last_reset"]
            
            if time_since_reset > 0:
                # Calculate rates
                ws_rate = self._performance_metrics["ws_updates_received"] / (time_since_reset / 60)
                rest_rate = self._performance_metrics["rest_polls_executed"] / (time_since_reset / 60)
                skip_rate = self._performance_metrics["rest_polls_skipped"] / (time_since_reset / 60)
                
                # Calculate efficiency
                total_potential_polls = self._performance_metrics["rest_polls_executed"] + self._performance_metrics["rest_polls_skipped"]
                efficiency_pct = (self._performance_metrics["rest_polls_skipped"] / total_potential_polls * 100) if total_potential_polls > 0 else 0
                
                self.logger().info(
                    f"XT Performance Metrics (last {time_since_reset:.0f}s): "
                    f"WS updates: {self._performance_metrics['ws_updates_received']} ({ws_rate:.1f}/min), "
                    f"REST polls executed: {self._performance_metrics['rest_polls_executed']} ({rest_rate:.1f}/min), "
                    f"REST polls skipped: {self._performance_metrics['rest_polls_skipped']} ({skip_rate:.1f}/min), "
                    f"Fill fetches: {self._performance_metrics['fill_fetches_triggered']}, "
                    f"Efficiency: {efficiency_pct:.1f}% REST calls avoided"
                )
                
                # Reset metrics every hour to avoid overflow
                if time_since_reset > 3600:
                    self._performance_metrics = {
                        "ws_updates_received": 0,
                        "rest_polls_executed": 0,
                        "rest_polls_skipped": 0,
                        "fill_fetches_triggered": 0,
                        "last_reset": self.current_timestamp
                    }
    
    async def start_network(self):
        """
        Override parent start_network to add performance metrics logging task.
        """
        await super().start_network()
        
        # Start performance metrics logging
        if self._trading_required:
            self._performance_metrics["last_reset"] = self.current_timestamp
            safe_ensure_future(self._log_performance_metrics())
            self.logger().info("XT WebSocket optimization enabled - Performance metrics logging started")
