import asyncio
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate

if TYPE_CHECKING:
    from hummingbot.connector.exchange.xt.xt_exchange import XtExchange


class BatchOrderStatusRequest:
    """
    Handles debouncing and batching of order status requests to optimize API calls.
    Collects multiple _request_order_status calls within a debounce window and
    executes them as a single batch API request.
    """
    BATCH_SIZE_LIMIT = 20  # XT API batch endpoint limit
    
    def __init__(self, exchange: 'XtExchange', debounce_window: float = 0.5):
        self._exchange = exchange
        self._debounce_window = debounce_window  # 500ms default
        self._pending_requests: Dict[str, Tuple[InFlightOrder, asyncio.Future]] = {}
        self._batch_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    async def request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Queue an order status request and return a future that will be resolved
        when the batch request completes.
        """
        should_fire_immediately = False
        
        async with self._lock:
            # Create a future for this specific request
            future = asyncio.Future()
            self._pending_requests[tracked_order.client_order_id] = (tracked_order, future)
            
            # Check if we've reached the batch size limit
            if len(self._pending_requests) >= self.BATCH_SIZE_LIMIT:
                should_fire_immediately = True
                # Cancel existing batch task if running
                if self._batch_task is not None and not self._batch_task.done():
                    self._batch_task.cancel()
            
            # Start batch processing if not already running or fire immediately
            if should_fire_immediately or self._batch_task is None or self._batch_task.done():
                self._batch_task = asyncio.create_task(
                    self._process_batch(immediate=should_fire_immediately)
                )
        
        # Wait for the batch to complete and return result
        return await future
    
    async def _process_batch(self, immediate: bool = False):
        """
        Wait for debounce window (unless immediate=True), then process all pending requests as a batch.
        
        :param immediate: If True, skip the debounce wait and process immediately
        """
        if not immediate:
            try:
                await asyncio.sleep(self._debounce_window)
            except asyncio.CancelledError:
                # Task was cancelled because batch size limit was reached
                return
        
        async with self._lock:
            if not self._pending_requests:
                return
            
            # Take snapshot of pending requests
            requests_to_process = self._pending_requests.copy()
            self._pending_requests.clear()
        
        # Separate orders with and without exchange IDs
        orders_with_exchange_ids = []
        orders_without_exchange_ids = []
        
        for client_order_id, (order, future) in requests_to_process.items():
            if order.exchange_order_id:
                orders_with_exchange_ids.append((order, future))
            else:
                orders_without_exchange_ids.append((order, future))
        
        # Handle orders without exchange IDs - return None (similar to original implementation)
        for order, future in orders_without_exchange_ids:
            if not future.done():
                future.set_result(None)
        
        if not orders_with_exchange_ids:
            return
        
        # Process in chunks (XT API batch size limit)
        for i in range(0, len(orders_with_exchange_ids), self.BATCH_SIZE_LIMIT):
            chunk = orders_with_exchange_ids[i:i + self.BATCH_SIZE_LIMIT]
            await self._process_batch_chunk(chunk)
    
    async def _process_batch_chunk(self, chunk: List[Tuple[InFlightOrder, asyncio.Future]]):
        """
        Process a chunk of order status requests using the batch API endpoint.
        """
        try:
            # Prepare batch request
            exchange_order_ids = ",".join([str(int(order.exchange_order_id)) for order, _ in chunk])
            
            response = await self._exchange._api_get(
                path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                params={"orderIds": exchange_order_ids},
                is_auth_required=True,
                limit_id=CONSTANTS.BATCH_ORDER_PATH_URL)
            
            # Handle no result case
            if "result" not in response or response["result"] is None:
                for order, future in chunk:
                    if not future.done():
                        future.set_result(None)
                return
            
            result_updates = response["result"]
            
            # Map results by client_order_id
            results_by_client_id = {}
            for result_update in result_updates:
                if result_update is not None:
                    client_order_id = result_update.get("clientOrderId")
                    if client_order_id:
                        results_by_client_id[client_order_id] = result_update
            
            # Resolve futures with results
            for order, future in chunk:
                if future.done():
                    continue
                
                client_order_id = order.client_order_id
                
                if client_order_id in results_by_client_id:
                    updated_order_data = results_by_client_id[client_order_id]
                    new_state = CONSTANTS.ORDER_STATE[updated_order_data["state"]]
                    
                    # Handle canceled orders
                    if new_state == OrderState.CANCELED:
                        try:
                            await self._exchange._cancelled_order_handler(client_order_id, updated_order_data)
                        except Exception as e:
                            self._exchange.logger().error(f"Error in cancelled order handler: {e}")
                    
                    # Create order update
                    order_update = OrderUpdate(
                        client_order_id=client_order_id,
                        exchange_order_id=str(updated_order_data["orderId"]),
                        trading_pair=order.trading_pair,
                        update_timestamp=(updated_order_data["updatedTime"] * 1e-3) if "updatedTime" in updated_order_data and updated_order_data["updatedTime"] is not None else None,
                        new_state=new_state,
                    )
                    future.set_result(order_update)
                else:
                    # Order not found in response
                    future.set_result(None)
        
        except Exception as e:
            # On error, resolve all futures with exception
            for order, future in chunk:
                if not future.done():
                    future.set_exception(e)

