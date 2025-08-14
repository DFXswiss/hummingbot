import logging
from decimal import Decimal
from typing import Dict, List

from hummingbot.strategy.pure_market_making.pure_market_making cimport PureMarketMakingStrategy
from hummingbot.strategy.pure_market_making.pure_market_making import PureMarketMakingStrategy
from hummingbot.strategy.hanging_orders_tracker import CreatedPairOfOrders
from hummingbot.core.data_type.limit_order cimport LimitOrder

NaN = float("nan")
s_decimal_zero = Decimal(0)
pmm_smart_logger = None


cdef class PureMarketMakingSmartStrategy(PureMarketMakingStrategy):
    """
    Enhanced Pure Market Making Strategy with Smart Order Refresh.
    
    This strategy extends the base PureMarketMakingStrategy to implement intelligent
    order management that only cancels and creates orders when necessary, preserving
    queue position and reducing fees.
    """

    @classmethod
    def logger(cls):
        global pmm_smart_logger
        if pmm_smart_logger is None:
            pmm_smart_logger = logging.getLogger(__name__)
        return pmm_smart_logger

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger().info("Smart Pure Market Making Strategy initialized - Orders will be refreshed intelligently")

    cdef c_cancel_active_orders(self, object proposal):
        """
        Smart order refresh: Only cancels orders that no longer meet conditions.
        Keeps orders that are still within tolerance threshold.
        """
        if self._cancel_timestamp > self._current_timestamp:
            return

        cdef:
            list active_orders = self.active_non_hanging_orders
            list orders_to_cancel = []
            list orders_to_keep = []
            dict proposal_prices_by_side = {}
            
        if len(active_orders) == 0:
            return
            
        # Build proposal price lookup for easy comparison
        if proposal is not None and self._order_refresh_tolerance_pct >= 0:
            proposal_prices_by_side = {
                'buy': [buy.price for buy in proposal.buys],
                'sell': [sell.price for sell in proposal.sells]
            }
            
            # Check each order individually
            for order in active_orders:
                side = 'buy' if order.is_buy else 'sell'
                proposal_prices = proposal_prices_by_side.get(side, [])
                
                # Check if this specific order is within tolerance of any proposal price
                order_within_tolerance = False
                order_price = Decimal(str(order.price))
                
                for proposal_price in proposal_prices:
                    if abs(proposal_price - order_price) / order_price <= self._order_refresh_tolerance_pct:
                        order_within_tolerance = True
                        break
                
                if order_within_tolerance:
                    orders_to_keep.append(order)
                    self.logger().debug(f"[SMART] Keeping order {order.client_order_id} at price {order_price} - within tolerance")
                else:
                    orders_to_cancel.append(order)
                    self.logger().debug(f"[SMART] Will cancel order {order.client_order_id} at price {order_price} - outside tolerance")
        else:
            # If no proposal or tolerance check disabled, cancel all as before
            orders_to_cancel = active_orders
            
        # Only cancel orders that don't meet conditions
        if orders_to_cancel:
            self._hanging_orders_tracker.update_strategy_orders_with_equivalent_orders()
            for order in orders_to_cancel:
                # If is about to be added to hanging_orders then don't cancel
                if not self._hanging_orders_tracker.is_potential_hanging_order(order):
                    self.c_cancel_order(self._market_info, order.client_order_id)
                    
        # Log efficiency metrics
        if len(active_orders) > 0:
            kept_ratio = len(orders_to_keep) / len(active_orders) * 100
            self.logger().info(f"[SMART] Order refresh: Kept {len(orders_to_keep)}/{len(active_orders)} orders ({kept_ratio:.1f}% efficiency)")

    cdef bint c_to_create_orders(self, object proposal):
        """
        Smart refresh: Create orders if we have missing positions or some orders were cancelled.
        Don't require ALL orders to be cancelled anymore.
        """
        non_hanging_orders_non_cancelled = [o for o in self.active_non_hanging_orders if not
                                            self._hanging_orders_tracker.is_potential_hanging_order(o)]
        # Changed condition: Always allow creating orders when needed
        return (self._create_timestamp < self._current_timestamp
                and (not self._should_wait_order_cancel_confirmation or
                     len(self._sb_order_tracker.in_flight_cancels) == 0)
                and proposal is not None)

    cdef c_execute_orders_proposal(self, object proposal):
        """
        Smart order creation: Only create orders that don't already exist at similar prices.
        """
        cdef:
            double expiration_seconds = NaN
            str bid_order_id, ask_order_id
            bint orders_created = False
            list active_orders = self.active_non_hanging_orders
            list active_buy_prices = []
            list active_sell_prices = []
            list buys_to_create = []
            list sells_to_create = []
            
        # Number of pair of orders to track for hanging orders
        number_of_pairs = min((len(proposal.buys), len(proposal.sells))) if self._hanging_orders_enabled else 0
        
        # Get current active order prices
        active_buy_prices = [Decimal(str(o.price)) for o in active_orders if o.is_buy]
        active_sell_prices = [Decimal(str(o.price)) for o in active_orders if not o.is_buy]
        
        # Smart order creation: Only create orders that don't already exist at similar prices
        for buy in proposal.buys:
            order_exists = False
            for active_price in active_buy_prices:
                if abs(buy.price - active_price) / active_price <= self._order_refresh_tolerance_pct:
                    order_exists = True
                    self.logger().debug(f"[SMART] Buy order at {buy.price} already exists (active: {active_price}), skipping")
                    break
            if not order_exists:
                buys_to_create.append(buy)
                
        for sell in proposal.sells:
            order_exists = False
            for active_price in active_sell_prices:
                if abs(sell.price - active_price) / active_price <= self._order_refresh_tolerance_pct:
                    order_exists = True
                    self.logger().debug(f"[SMART] Sell order at {sell.price} already exists (active: {active_price}), skipping")
                    break
            if not order_exists:
                sells_to_create.append(sell)

        # Create only missing buy orders
        if len(buys_to_create) > 0:
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                price_quote_str = [f"{buy.size.normalize()} {self.base_asset}, "
                                   f"{buy.price.normalize()} {self.quote_asset}"
                                   for buy in buys_to_create]
                self.logger().info(
                    f"[SMART] ({self.trading_pair}) Creating {len(buys_to_create)} NEW bid orders "
                    f"at (Size, Price): {price_quote_str} "
                    f"(Kept {len(proposal.buys) - len(buys_to_create)} existing orders)"
                )
            for idx, buy in enumerate(buys_to_create):
                bid_order_id = self.c_buy_with_specific_market(
                    self._market_info,
                    buy.size,
                    order_type=self._limit_order_type,
                    price=buy.price,
                    expiration_seconds=expiration_seconds
                )
                orders_created = True
                if idx < number_of_pairs:
                    order = next((o for o in self.active_orders if o.client_order_id == bid_order_id), None)
                    if order:
                        self._hanging_orders_tracker.add_current_pairs_of_proposal_orders_executed_by_strategy(
                            CreatedPairOfOrders(order, None))
                            
        # Create only missing sell orders
        if len(sells_to_create) > 0:
            if self._logging_options & self.OPTION_LOG_CREATE_ORDER:
                price_quote_str = [f"{sell.size.normalize()} {self.base_asset}, "
                                   f"{sell.price.normalize()} {self.quote_asset}"
                                   for sell in sells_to_create]
                self.logger().info(
                    f"[SMART] ({self.trading_pair}) Creating {len(sells_to_create)} NEW ask orders "
                    f"at (Size, Price): {price_quote_str} "
                    f"(Kept {len(proposal.sells) - len(sells_to_create)} existing orders)"
                )
            for idx, sell in enumerate(sells_to_create):
                ask_order_id = self.c_sell_with_specific_market(
                    self._market_info,
                    sell.size,
                    order_type=self._limit_order_type,
                    price=sell.price,
                    expiration_seconds=expiration_seconds
                )
                orders_created = True
                if idx < number_of_pairs and idx < len(self._hanging_orders_tracker.current_created_pairs_of_orders):
                    order = next((o for o in self.active_orders if o.client_order_id == ask_order_id), None)
                    if order:
                        self._hanging_orders_tracker.current_created_pairs_of_orders[idx].sell_order = order
                        
        # Log summary of operations
        total_kept = (len(proposal.buys) - len(buys_to_create)) + (len(proposal.sells) - len(sells_to_create))
        total_created = len(buys_to_create) + len(sells_to_create)
        if total_kept > 0 or total_created > 0:
            self.logger().info(
                f"[SMART] Order management complete: {total_kept} orders kept, {total_created} new orders created"
            )
                        
        if orders_created:
            self.set_timers()