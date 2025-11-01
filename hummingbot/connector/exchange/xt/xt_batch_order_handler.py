import asyncio
from decimal import Decimal
from typing import Dict, List, Tuple, Any
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.connector.exchange.xt import xt_constants as CONSTANTS


class BatchOrderHandler:
    """
    Handles batching of place and cancel order requests.
    Fires after 300ms OR when 20 orders are collected.
    """
    
    def __init__(self, exchange):
        self._exchange = exchange
        
        # Place order batching
        self._place_queue: List[Tuple[Dict[str, Any], asyncio.Future]] = []
        self._place_timer: asyncio.Task = None
        self._place_lock = asyncio.Lock()
        self._place_flushing = False
        
        # Cancel order batching
        self._cancel_queue: List[Tuple[Dict[str, Any], asyncio.Future]] = []
        self._cancel_timer: asyncio.Task = None
        self._cancel_lock = asyncio.Lock()
        self._cancel_flushing = False
        
        # Configuration
        self._batch_size = 20
        self._debounce_time = 0.3  # 300ms
    
    async def place_order(self,
                         order_id: str,
                         trading_pair: str,
                         amount: Decimal,
                         trade_type: TradeType,
                         order_type: OrderType,
                         price: Decimal,
                         **kwargs) -> Tuple[str, float]:
        """
        Queue a place order request. Returns when the order is placed.
        Transparent to caller - behaves like synchronous call.
        """
        # Prepare order params
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        type_str = self._exchange.xt_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self._exchange.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        
        api_params = {
            "symbol": symbol,
            "side": side_str,
            "quantity": amount_str,
            "type": type_str,
            "clientOrderId": order_id,
            "bizType": "SPOT",
            "price": price_str
        }
        
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC
        
        # Create future for this order
        future = asyncio.Future()
        
        # Check if we need to wait for a flush to complete
        should_flush_now = False
        
        async with self._place_lock:
            # Always queue the order
            self._place_queue.append((api_params, future))
            
            self._exchange.logger().info(
                f"[BATCH PLACE ORDER] Queued - order_id: {order_id}, symbol: {symbol}, "
                f"side: {side_str}, type: {type_str}, amount: {amount_str}, price: {price_str}, "
                f"queue_size: {len(self._place_queue)}, flushing: {self._place_flushing}"
            )
            
            # Check if we should flush immediately
            if not self._place_flushing and len(self._place_queue) >= self._batch_size:
                should_flush_now = True
                self._place_flushing = True
                # Cancel timer if exists
                if self._place_timer and not self._place_timer.done():
                    self._place_timer.cancel()
                    self._place_timer = None
            elif not self._place_flushing:
                # Start/reset timer if not at batch size and not flushing
                if self._place_timer is None or self._place_timer.done():
                    self._place_timer = asyncio.create_task(self._place_debounce_timer())
        
        # Flush immediately if needed (outside lock to avoid deadlock)
        if should_flush_now:
            await self._flush_place_queue()
        
        # Wait for result
        return await future
    
    async def _place_debounce_timer(self):
        """Wait for debounce time then flush"""
        try:
            await asyncio.sleep(self._debounce_time)
            await self._flush_place_queue()
        except asyncio.CancelledError:
            pass
    
    async def _flush_place_queue(self):
        """Execute queued place orders as batch (up to batch_size at a time)"""
        # Loop until queue is empty or below batch size
        while True:
            async with self._place_lock:
                if not self._place_queue:
                    self._place_flushing = False
                    return
                
                # Take up to batch_size orders
                batch = self._place_queue[:self._batch_size]
                self._place_queue = self._place_queue[self._batch_size:]
                self._place_timer = None
            
            if len(batch) == 0:
                async with self._place_lock:
                    self._place_flushing = False
                return
            
            self._exchange.logger().info(
                f"[BATCH PLACE ORDER] Flushing {len(batch)} orders (remaining in queue: {len(self._place_queue)})"
            )
            
            try:
                # Always use batch endpoint (even for single orders)
                items = [params for params, _ in batch]
                self._exchange.logger().info(f"[BATCH PLACE ORDER] Sending batch of {len(batch)} orders")
                result = await self._exchange._api_post(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    data={"items": items},
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL
                )
                
                # Process results
                if "result" not in result or result["result"] is None:
                    # All failed
                    self._exchange.logger().error(f"[BATCH PLACE ORDER ERROR] No result in response: {result}")
                    error = IOError(f"Batch order failed. API response: {result}")
                    for _, future in batch:
                        if not future.done():
                            future.set_exception(error)
                else:
                    # Map results back to futures
                    transact_time = self._exchange.current_timestamp
                    batch_response = result["result"]
                    
                    # Check if result is a dict with items or a list directly
                    if isinstance(batch_response, dict):
                        results_list = batch_response.get("items", [])
                    elif isinstance(batch_response, list):
                        results_list = batch_response
                    else:
                        self._exchange.logger().error(f"[BATCH PLACE ORDER ERROR] Unexpected result format: {batch_response}")
                        error = IOError(f"Unexpected batch response format: {type(batch_response)}")
                        for _, future in batch:
                            if not future.done():
                                future.set_exception(error)
                        return
                    
                    self._exchange.logger().info(
                        f"[BATCH PLACE ORDER RESPONSE] {len(results_list)} results for {len(batch)} orders"
                    )
                    
                    # Create mapping by clientOrderId
                    result_map = {}
                    for r in results_list:
                        if r and "clientOrderId" in r:
                            result_map[r["clientOrderId"]] = r
                        else:
                            self._exchange.logger().warning(f"[BATCH PLACE ORDER] Result missing clientOrderId: {r}")
                    
                    # Set results for each future
                    for params, future in batch:
                        if not future.done():
                            client_order_id = params["clientOrderId"]
                            if client_order_id in result_map:
                                order_result = result_map[client_order_id]
                                if "orderId" in order_result:
                                    o_id = str(order_result["orderId"])
                                    future.set_result((o_id, transact_time))
                                else:
                                    error_msg = f"Order {client_order_id} missing orderId in result: {order_result}"
                                    self._exchange.logger().error(f"[BATCH PLACE ORDER ERROR] {error_msg}")
                                    future.set_exception(IOError(error_msg))
                            else:
                                error_msg = f"Order {client_order_id} not found in batch response (got {len(result_map)} results)"
                                self._exchange.logger().error(f"[BATCH PLACE ORDER ERROR] {error_msg}")
                                future.set_exception(IOError(error_msg))
            
            except Exception as e:
                # Set exception for all pending futures
                self._exchange.logger().error(f"Batch place order error: {e}", exc_info=True)
                for _, future in batch:
                    if not future.done():
                        future.set_exception(e)
            
            # After each batch, check if we should continue looping
            async with self._place_lock:
                if len(self._place_queue) < self._batch_size:
                    # Less than batch size remaining, set up timer and exit loop
                    self._place_flushing = False
                    if len(self._place_queue) > 0:
                        if self._place_timer is None or self._place_timer.done():
                            self._place_timer = asyncio.create_task(self._place_debounce_timer())
                    return
    
    async def cancel_order(self, order_id: str, exchange_order_id: str):
        """
        Queue a cancel order request. Returns when the order is cancelled.
        Transparent to caller - behaves like synchronous call.
        """
        # Create future for this cancellation
        future = asyncio.Future()
        
        should_flush_now = False
        
        async with self._cancel_lock:
            # Always queue the cancellation
            self._cancel_queue.append(({"order_id": order_id, "exchange_order_id": exchange_order_id}, future))
            
            self._exchange.logger().info(
                f"[BATCH CANCEL ORDER] Queued - client_order_id: {order_id}, "
                f"exchange_order_id: {exchange_order_id}, queue_size: {len(self._cancel_queue)}, "
                f"flushing: {self._cancel_flushing}"
            )
            
            # Check if we should flush immediately
            if not self._cancel_flushing and len(self._cancel_queue) >= self._batch_size:
                should_flush_now = True
                self._cancel_flushing = True
                # Cancel timer if exists
                if self._cancel_timer and not self._cancel_timer.done():
                    self._cancel_timer.cancel()
                    self._cancel_timer = None
            elif not self._cancel_flushing:
                # Start/reset timer if not at batch size and not flushing
                if self._cancel_timer is None or self._cancel_timer.done():
                    self._cancel_timer = asyncio.create_task(self._cancel_debounce_timer())
        
        # Flush immediately if needed (outside lock to avoid deadlock)
        if should_flush_now:
            await self._flush_cancel_queue()
        
        # Wait for result
        await future
    
    async def _cancel_debounce_timer(self):
        """Wait for debounce time then flush"""
        try:
            await asyncio.sleep(self._debounce_time)
            await self._flush_cancel_queue()
        except asyncio.CancelledError:
            pass
    
    async def _flush_cancel_queue(self):
        """Execute queued cancel orders as batch (up to batch_size at a time)"""
        # Loop until queue is empty or below batch size
        while True:
            async with self._cancel_lock:
                if not self._cancel_queue:
                    self._cancel_flushing = False
                    return
                
                # Take up to batch_size cancellations
                batch = self._cancel_queue[:self._batch_size]
                self._cancel_queue = self._cancel_queue[self._batch_size:]
                self._cancel_timer = None
            
            if len(batch) == 0:
                async with self._cancel_lock:
                    self._cancel_flushing = False
                return
            
            self._exchange.logger().info(
                f"[BATCH CANCEL ORDER] Flushing {len(batch)} cancellations (remaining in queue: {len(self._cancel_queue)})"
            )
            
            try:
                # Always use batch endpoint (even for single cancellation)
                order_ids = [int(params["exchange_order_id"]) for params, _ in batch]
                
                self._exchange.logger().info(
                    f"[BATCH CANCEL ORDER] Sending batch cancel for {len(order_ids)} orders: {order_ids[:5]}..."
                )
                
                # Use DELETE with body data (auth.py now handles this correctly)
                result = await self._exchange._api_delete(
                    path_url=CONSTANTS.BATCH_ORDER_PATH_URL,
                    data={"orderIds": order_ids},
                    is_auth_required=True,
                    limit_id=CONSTANTS.BATCH_ORDER_PATH_URL
                )
                
                # Log the actual API response
                self._exchange.logger().info(
                    f"[BATCH CANCEL ORDER RESPONSE] API result: {result}"
                )
                
                # Check if the response indicates success
                if "result" in result and result["result"] is not None:
                    self._exchange.logger().info(
                        f"[BATCH CANCEL ORDER RESPONSE] Success for {len(batch)} cancellations"
                    )
                    # Mark all as successful
                    for _, future in batch:
                        if not future.done():
                            future.set_result(None)
                else:
                    # API returned an error or no result
                    error_msg = f"Batch cancel failed. API response: {result}"
                    self._exchange.logger().error(f"[BATCH CANCEL ORDER ERROR] {error_msg}")
                    error = IOError(error_msg)
                    for _, future in batch:
                        if not future.done():
                            future.set_exception(error)
            
            except Exception as e:
                # Set exception for all pending futures
                self._exchange.logger().error(f"[BATCH CANCEL ORDER ERROR] {e}", exc_info=True)
                for _, future in batch:
                    if not future.done():
                        future.set_exception(e)
            
            # After each batch, check if we should continue looping
            async with self._cancel_lock:
                if len(self._cancel_queue) < self._batch_size:
                    # Less than batch size remaining, set up timer and exit loop
                    self._cancel_flushing = False
                    if len(self._cancel_queue) > 0:
                        if self._cancel_timer is None or self._cancel_timer.done():
                            self._cancel_timer = asyncio.create_task(self._cancel_debounce_timer())
                    return

