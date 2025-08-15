# Improved order management that never leaves the order book empty
# This is a draft for the improved cancel_active_orders function

cdef c_cancel_active_orders(self, object proposal):
    """
    Smart order cancellation that ensures order book is never empty.
    Only cancels orders that need updating, and does it in batches to maintain liquidity.
    """
    if self._cancel_timestamp > self._current_timestamp:
        return
        
    cdef:
        list active_orders = self.active_non_hanging_orders
        list orders_to_cancel = []
        list critical_orders = []  # Orders we must keep until replacements are ready
        int min_orders_to_keep = 5  # Always keep at least 5 orders per side
        
    if len(active_orders) == 0:
        return
        
    if proposal is None or self._order_refresh_tolerance_pct < 0:
        # No tolerance checking - use staggered cancellation
        # Never cancel all at once
        self._hanging_orders_tracker.update_strategy_orders_with_equivalent_orders()
        
        active_buys = [o for o in active_orders if o.is_buy]
        active_sells = [o for o in active_orders if not o.is_buy]
        
        # Keep minimum orders on each side
        buys_to_cancel = active_buys[min_orders_to_keep:] if len(active_buys) > min_orders_to_keep else []
        sells_to_cancel = active_sells[min_orders_to_keep:] if len(active_sells) > min_orders_to_keep else []
        
        for order in buys_to_cancel + sells_to_cancel:
            if not self._hanging_orders_tracker.is_potential_hanging_order(order):
                self.c_cancel_order(self._market_info, order.client_order_id)
        return
    
    # Smart cancellation with tolerance checking
    active_buys = sorted([o for o in active_orders if o.is_buy], 
                        key=lambda x: x.price, reverse=True)
    active_sells = sorted([o for o in active_orders if not o.is_buy], 
                         key=lambda x: x.price)
    proposal_buys = sorted(proposal.buys, key=lambda x: x.price, reverse=True)
    proposal_sells = sorted(proposal.sells, key=lambda x: x.price)
    
    # Determine which orders are critical (closest to mid price)
    if len(active_buys) > 0:
        critical_buy_orders = active_buys[:min_orders_to_keep]
    else:
        critical_buy_orders = []
        
    if len(active_sells) > 0:
        critical_sell_orders = active_sells[:min_orders_to_keep]
    else:
        critical_sell_orders = []
    
    # Check each order but protect critical orders
    for order in active_orders:
        if order in critical_buy_orders or order in critical_sell_orders:
            # Only cancel critical orders if severely out of tolerance (3x normal)
            severe_tolerance = self._order_refresh_tolerance_pct * 3
            
            # Find closest proposal price
            if order.is_buy:
                min_diff = min([abs(order.price - p.price) for p in proposal_buys]) if proposal_buys else Decimal("999999")
                closest_price = min([p.price for p in proposal_buys], key=lambda x: abs(x - order.price)) if proposal_buys else order.price
            else:
                min_diff = min([abs(order.price - p.price) for p in proposal_sells]) if proposal_sells else Decimal("999999")
                closest_price = min([p.price for p in proposal_sells], key=lambda x: abs(x - order.price)) if proposal_sells else order.price
            
            if closest_price > 0:
                price_diff = min_diff / closest_price
                if price_diff > severe_tolerance:
                    orders_to_cancel.append(order)
        else:
            # Non-critical orders use normal tolerance
            if order.is_buy:
                min_diff = min([abs(order.price - p.price) for p in proposal_buys]) if proposal_buys else Decimal("999999")
                closest_price = min([p.price for p in proposal_buys], key=lambda x: abs(x - order.price)) if proposal_buys else None
            else:
                min_diff = min([abs(order.price - p.price) for p in proposal_sells]) if proposal_sells else Decimal("999999")
                closest_price = min([p.price for p in proposal_sells], key=lambda x: abs(x - order.price)) if proposal_sells else None
            
            if closest_price and closest_price > 0:
                price_diff = min_diff / closest_price
                if price_diff > self._order_refresh_tolerance_pct:
                    orders_to_cancel.append(order)
            else:
                orders_to_cancel.append(order)
    
    # Staggered cancellation - never cancel everything at once
    if len(orders_to_cancel) > 0:
        # Calculate how many we can safely cancel
        remaining_buys = len([o for o in active_orders if o.is_buy and o not in orders_to_cancel])
        remaining_sells = len([o for o in active_orders if not o.is_buy and o not in orders_to_cancel])
        
        # Sort orders to cancel by distance from mid price (cancel furthest first)
        current_price = self.get_price()
        orders_to_cancel.sort(key=lambda x: abs(x.price - current_price), reverse=True)
        
        # Only cancel orders if we'll still have minimum coverage
        safe_to_cancel = []
        for order in orders_to_cancel:
            if order.is_buy:
                if remaining_buys > min_orders_to_keep:
                    safe_to_cancel.append(order)
                    remaining_buys -= 1
            else:
                if remaining_sells > min_orders_to_keep:
                    safe_to_cancel.append(order)
                    remaining_sells -= 1
        
        if len(safe_to_cancel) > 0:
            if self._logging_options & self.OPTION_LOG_ADJUST_ORDER:
                self.logger().info(
                    f"Safely cancelling {len(safe_to_cancel)} of {len(active_orders)} orders "
                    f"(keeping minimum {min_orders_to_keep} per side)"
                )
            
            self._hanging_orders_tracker.update_strategy_orders_with_equivalent_orders()
            for order in safe_to_cancel:
                if not self._hanging_orders_tracker.is_potential_hanging_order(order):
                    self.c_cancel_order(self._market_info, order.client_order_id)