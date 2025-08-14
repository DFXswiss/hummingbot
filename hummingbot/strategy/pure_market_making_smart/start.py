from decimal import Decimal
from typing import List, Tuple

from hummingbot import data_path
from hummingbot.client.hummingbot_application import HummingbotApplication
from hummingbot.strategy.pure_market_making_smart import PureMarketMakingSmartStrategy
from hummingbot.strategy.pure_market_making.pure_market_making_config_map import pure_market_making_config_map as c_map


def start(self: HummingbotApplication):
    """
    Start the Pure Market Making Smart strategy.
    This strategy inherits all configuration from pure_market_making but uses
    intelligent order refresh that only cancels/creates orders when necessary.
    """
    try:
        exchange = c_map.get("exchange").value.lower()
        raw_trading_pair = c_map.get("market").value
        
        bid_spread = c_map.get("bid_spread").value / Decimal('100')
        ask_spread = c_map.get("ask_spread").value / Decimal('100')
        minimum_spread = c_map.get("minimum_spread").value / Decimal('100')
        order_refresh_time = c_map.get("order_refresh_time").value
        order_amount = c_map.get("order_amount").value
        order_refresh_tolerance_pct = c_map.get("order_refresh_tolerance_pct").value / Decimal('100')
        order_levels = c_map.get("order_levels").value
        order_level_amount = c_map.get("order_level_amount").value
        order_level_spread = c_map.get("order_level_spread").value / Decimal('100')
        inventory_skew_enabled = c_map.get("inventory_skew_enabled").value
        inventory_target_base_pct = 0 if not inventory_skew_enabled else c_map.get("inventory_target_base_pct").value / Decimal('100')
        inventory_range_multiplier = 0 if not inventory_skew_enabled else c_map.get("inventory_range_multiplier").value
        filled_order_delay = c_map.get("filled_order_delay").value
        hanging_orders_enabled = c_map.get("hanging_orders_enabled").value
        hanging_orders_cancel_pct = 0 if not hanging_orders_enabled else c_map.get("hanging_orders_cancel_pct").value / Decimal('100')
        order_optimization_enabled = c_map.get("order_optimization_enabled").value
        order_optimization_depth = 0 if not order_optimization_enabled else c_map.get("order_optimization_depth").value
        add_transaction_costs = c_map.get("add_transaction_costs").value
        price_source = c_map.get("price_source").value
        price_type = c_map.get("price_type").value
        price_source_exchange = c_map.get("price_source_exchange").value
        price_source_market = c_map.get("price_source_market").value
        price_source_custom_api = c_map.get("price_source_custom_api").value
        custom_api_update_interval = c_map.get("custom_api_update_interval").value
        order_override = c_map.get("order_override").value
        split_order_levels_enabled = c_map.get("split_order_levels_enabled").value
        bid_order_level_spreads = c_map.get("bid_order_level_spreads").value if split_order_levels_enabled else None
        ask_order_level_spreads = c_map.get("ask_order_level_spreads").value if split_order_levels_enabled else None
        bid_order_level_amounts = c_map.get("bid_order_level_amounts").value if split_order_levels_enabled else None
        ask_order_level_amounts = c_map.get("ask_order_level_amounts").value if split_order_levels_enabled else None
        should_wait_order_cancel_confirmation = c_map.get("should_wait_order_cancel_confirmation").value
        
        trading_pair: str = raw_trading_pair
        maker_assets: Tuple[str, str] = self._initialize_market_assets(exchange, [trading_pair])[0]
        market_names: List[Tuple[str, List[str]]] = [(exchange, [trading_pair])]
        
        self._initialize_markets(market_names)
        maker_data = [self.markets[exchange], trading_pair] + list(maker_assets)
        self.market_trading_pair_tuples = [maker_data]
        
        asset_price_delegate = None
        if price_source == "external_market":
            asset_price_delegate = self._initialize_asset_price_delegate(exchange, trading_pair, price_source,
                                                                          price_source_exchange, price_source_market,
                                                                          price_type)
        elif price_source == "custom_api":
            asset_price_delegate = self._initialize_custom_asset_price_delegate(price_source_custom_api,
                                                                                 custom_api_update_interval)
        
        take_if_crossed = c_map.get("take_if_crossed").value
        
        moving_price_band_enabled = c_map.get("moving_price_band_enabled").value
        moving_price_band_n = c_map.get("moving_price_band_n").value if moving_price_band_enabled else None
        moving_price_band_price_floor = c_map.get("moving_price_band_price_floor").value / Decimal(100) \
            if moving_price_band_enabled else None
        moving_price_band_price_ceiling = c_map.get("moving_price_band_price_ceiling").value / Decimal(100) \
            if moving_price_band_enabled else None
        
        ping_pong_enabled = c_map.get("ping_pong_enabled").value
        
        strategy_logging_options = PureMarketMakingSmartStrategy.OPTION_LOG_ALL
        
        self.strategy = PureMarketMakingSmartStrategy()
        self.strategy.init_params(
            market_info=maker_data,
            bid_spread=bid_spread,
            ask_spread=ask_spread,
            order_levels=order_levels,
            order_amount=order_amount,
            order_level_spread=order_level_spread,
            order_level_amount=order_level_amount,
            inventory_skew_enabled=inventory_skew_enabled,
            inventory_target_base_pct=inventory_target_base_pct,
            inventory_range_multiplier=inventory_range_multiplier,
            filled_order_delay=filled_order_delay,
            hanging_orders_enabled=hanging_orders_enabled,
            order_refresh_time=order_refresh_time,
            order_refresh_tolerance_pct=order_refresh_tolerance_pct,
            minimum_spread=minimum_spread,
            price_ceiling=Decimal("0"),
            price_floor=Decimal("0"),
            ping_pong_enabled=ping_pong_enabled,
            logging_options=strategy_logging_options,
            asset_price_delegate=asset_price_delegate,
            price_type=price_type,
            take_if_crossed=take_if_crossed,
            add_transaction_costs=add_transaction_costs,
            order_override={},
            hanging_orders_cancel_pct=hanging_orders_cancel_pct,
            order_optimization_enabled=order_optimization_enabled,
            order_optimization_depth=order_optimization_depth,
            split_order_levels_enabled=split_order_levels_enabled,
            bid_order_level_spreads=bid_order_level_spreads,
            ask_order_level_spreads=ask_order_level_spreads,
            bid_order_level_amounts=bid_order_level_amounts,
            ask_order_level_amounts=ask_order_level_amounts,
            moving_price_band_enabled=moving_price_band_enabled,
            moving_price_band_n=moving_price_band_n,
            moving_price_band_price_floor=moving_price_band_price_floor,
            moving_price_band_price_ceiling=moving_price_band_price_ceiling,
            should_wait_order_cancel_confirmation=should_wait_order_cancel_confirmation,
        )
        
        self.logger().info(f"Smart Pure Market Making strategy started with intelligent order refresh enabled.")
        self.logger().info(f"Orders will only be cancelled/created when necessary, preserving queue position.")
        
    except Exception as e:
        self.notify(str(e))
        self.logger().error("Unknown error during initialization.", exc_info=True)