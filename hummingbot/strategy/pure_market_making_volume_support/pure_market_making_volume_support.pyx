import logging
from decimal import Decimal
from typing import Dict, List, Optional

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.strategy.pure_market_making import PureMarketMakingStrategy
from hummingbot.strategy.pure_market_making.moving_price_band import MovingPriceBand
from hummingbot.strategy.asset_price_delegate import AssetPriceDelegate
from hummingbot.strategy.pure_market_making.inventory_cost_price_delegate import InventoryCostPriceDelegate
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.connector.exchange_base cimport ExchangeBase

pmm_vs_logger = None

cdef class PureMarketMakingVolumeSupport(PureMarketMakingStrategy):
    """
    Pure Market Making Strategy with Volume Support to ensure minimum transaction frequency.
    Extends the base PureMarketMakingStrategy with volume injection capabilities.
    """
    
    @classmethod
    def logger(cls):
        global pmm_vs_logger
        if pmm_vs_logger is None:
            pmm_vs_logger = logging.getLogger(__name__)
        return pmm_vs_logger

    def __init__(self):
        super().__init__()
        # Volume injection specific attributes will be set in init_params
        
    def init_params(self,
                    market_info: MarketTradingPairTuple,
                    bid_spread: Decimal,
                    ask_spread: Decimal,
                    order_amount: Decimal,
                    order_levels: int = 1,
                    order_level_spread: Decimal = Decimal(0),
                    order_level_amount: Decimal = Decimal(0),
                    order_refresh_time: float = 30.0,
                    max_order_age: float = 1800.0,
                    order_refresh_tolerance_pct: Decimal = Decimal(-1),
                    filled_order_delay: float = 60.0,
                    inventory_skew_enabled: bool = False,
                    inventory_target_base_pct: Decimal = Decimal(0),
                    inventory_range_multiplier: Decimal = Decimal(0),
                    hanging_orders_enabled: bool = False,
                    hanging_orders_cancel_pct: Decimal = Decimal("0.1"),
                    order_optimization_enabled: bool = False,
                    ask_order_optimization_depth: Decimal = Decimal(0),
                    bid_order_optimization_depth: Decimal = Decimal(0),
                    add_transaction_costs_to_orders: bool = False,
                    asset_price_delegate: AssetPriceDelegate = None,
                    inventory_cost_price_delegate: InventoryCostPriceDelegate = None,
                    price_type: str = "mid_price",
                    take_if_crossed: bool = False,
                    price_ceiling: Decimal = Decimal(-1),
                    price_floor: Decimal = Decimal(-1),
                    ping_pong_enabled: bool = False,
                    logging_options: int = 0x7fffffffffffffff,
                    status_report_interval: float = 900,
                    minimum_spread: Decimal = Decimal(0),
                    hb_app_notification: bool = False,
                    order_override: Dict[str, List[str]] = None,
                    split_order_levels_enabled: bool = False,
                    bid_order_level_spreads: List[Decimal] = None,
                    ask_order_level_spreads: List[Decimal] = None,
                    should_wait_order_cancel_confirmation: bool = True,
                    moving_price_band: Optional[MovingPriceBand] = None,
                    invert_custom_api_price: bool = False,
                    coin_id_overrides: Dict[str, str] = None,
                    header_custom_api: Dict[str, str] = None,
                    # Volume support specific parameters
                    volume_injection_enabled: bool = False,
                    min_transaction_interval: float = 60.0,
                    volume_injection_amount_pct: Decimal = Decimal("15"),
                    volume_injection_spread_pct: Decimal = Decimal("20")
                    ):
        
        # Initialize parent class
        super().init_params(
            market_info=market_info,
            bid_spread=bid_spread,
            ask_spread=ask_spread,
            order_amount=order_amount,
            order_levels=order_levels,
            order_level_spread=order_level_spread,
            order_level_amount=order_level_amount,
            order_refresh_time=order_refresh_time,
            max_order_age=max_order_age,
            order_refresh_tolerance_pct=order_refresh_tolerance_pct,
            filled_order_delay=filled_order_delay,
            inventory_skew_enabled=inventory_skew_enabled,
            inventory_target_base_pct=inventory_target_base_pct,
            inventory_range_multiplier=inventory_range_multiplier,
            hanging_orders_enabled=hanging_orders_enabled,
            hanging_orders_cancel_pct=hanging_orders_cancel_pct,
            order_optimization_enabled=order_optimization_enabled,
            ask_order_optimization_depth=ask_order_optimization_depth,
            bid_order_optimization_depth=bid_order_optimization_depth,
            add_transaction_costs_to_orders=add_transaction_costs_to_orders,
            asset_price_delegate=asset_price_delegate,
            inventory_cost_price_delegate=inventory_cost_price_delegate,
            price_type=price_type,
            take_if_crossed=take_if_crossed,
            price_ceiling=price_ceiling,
            price_floor=price_floor,
            ping_pong_enabled=ping_pong_enabled,
            logging_options=logging_options,
            status_report_interval=status_report_interval,
            minimum_spread=minimum_spread,
            hb_app_notification=hb_app_notification,
            order_override=order_override,
            split_order_levels_enabled=split_order_levels_enabled,
            bid_order_level_spreads=bid_order_level_spreads,
            ask_order_level_spreads=ask_order_level_spreads,
            should_wait_order_cancel_confirmation=should_wait_order_cancel_confirmation,
            moving_price_band=moving_price_band,
            invert_custom_api_price=invert_custom_api_price,
            coin_id_overrides=coin_id_overrides,
            header_custom_api=header_custom_api
        )
        
        # Initialize volume injection specific parameters
        self._volume_injection_enabled = volume_injection_enabled
        self._min_transaction_interval = min_transaction_interval
        self._volume_injection_amount_pct = volume_injection_amount_pct
        self._volume_injection_spread_pct = volume_injection_spread_pct
        self._last_transaction_timestamp = 0
        self._volume_injection_cooldown = 0
        
    cdef c_tick(self, double timestamp):
        """Override tick to add volume injection check"""
        # Call parent tick
        super().c_tick(timestamp)
        
        # Check volume injection conditions after parent processing
        if self._volume_injection_enabled:
            self.c_check_volume_injection(timestamp)
    
    cdef c_did_fill_order(self, object order_filled_event):
        """Override to track transaction timestamps"""
        # Call parent implementation
        super().c_did_fill_order(order_filled_event)
        
        # Update transaction timestamp for volume injection tracking
        if self._volume_injection_enabled:
            self._last_transaction_timestamp = self._current_timestamp
            self._volume_injection_cooldown = 0
            self.logger().debug(f"Transaction recorded at {self._current_timestamp}, resetting volume injection")
    
    cdef c_check_volume_injection(self, double timestamp):
        """Check if volume injection should be triggered"""
        if self._last_transaction_timestamp == 0:
            self._last_transaction_timestamp = timestamp
            return
            
        # Check if we're still in cooldown
        if timestamp < self._volume_injection_cooldown:
            return
            
        cdef double time_since_last_transaction = timestamp - self._last_transaction_timestamp
        
        # Trigger volume injection if approaching deadline (85% of interval)
        if time_since_last_transaction >= (self._min_transaction_interval * 0.85):
            self.logger().info(f"Volume injection triggered: {time_since_last_transaction:.1f}s since last transaction")
            self.c_create_volume_injection_orders()
            # Set cooldown to prevent multiple injections
            self._volume_injection_cooldown = timestamp + 30  # 30 second cooldown
            
    cdef c_create_volume_injection_orders(self):
        """Create small orders designed to generate transactions"""
        cdef:
            ExchangeBase market = self._market_info.market
            object reference_price = self.get_price()
            
        if reference_price.is_nan():
            self.logger().warning("Cannot create volume injection orders: price is NaN")
            return
            
        # Calculate volume injection parameters
        cdef:
            Decimal injection_amount = self._order_amount * (self._volume_injection_amount_pct / Decimal("100"))
            Decimal spread_reduction = self._volume_injection_spread_pct / Decimal("100")
            Decimal buy_spread = self._bid_spread * (Decimal("1") - spread_reduction)
            Decimal sell_spread = self._ask_spread * (Decimal("1") - spread_reduction)
            
        # Create tighter spreads for better fill probability
        cdef:
            Decimal buy_price = reference_price * (Decimal("1") - buy_spread)
            Decimal sell_price = reference_price * (Decimal("1") + sell_spread)
            
        buy_price = market.c_quantize_order_price(self.trading_pair, buy_price)
        sell_price = market.c_quantize_order_price(self.trading_pair, sell_price)
        injection_amount = market.c_quantize_order_amount(self.trading_pair, injection_amount)
        
        if injection_amount > 0:
            # Check balances before creating orders
            base_balance = market.c_get_available_balance(self.base_asset)
            quote_balance = market.c_get_available_balance(self.quote_asset)
            
            # Alternate between buy and sell based on timestamp
            if int(self._current_timestamp) % 2 == 0:
                # Create buy order if we have quote balance
                required_quote = injection_amount * buy_price
                if buy_price > 0 and quote_balance >= required_quote:
                    order_id = self.c_buy_with_specific_market(
                        self._market_info,
                        injection_amount,
                        order_type=self._limit_order_type,
                        price=buy_price
                    )
                    if order_id is not None:
                        self.logger().info(
                            f"Volume injection: Created buy order {order_id} "
                            f"for {injection_amount} {self.base_asset} at {buy_price} {self.quote_asset}"
                        )
                else:
                    self.logger().debug(f"Insufficient quote balance for volume injection buy order")
            else:
                # Create sell order if we have base balance
                if sell_price > 0 and base_balance >= injection_amount:
                    order_id = self.c_sell_with_specific_market(
                        self._market_info,
                        injection_amount,
                        order_type=self._limit_order_type,
                        price=sell_price
                    )
                    if order_id is not None:
                        self.logger().info(
                            f"Volume injection: Created sell order {order_id} "
                            f"for {injection_amount} {self.base_asset} at {sell_price} {self.quote_asset}"
                        )
                else:
                    self.logger().debug(f"Insufficient base balance for volume injection sell order")
    
    def format_status(self) -> str:
        """Override to add volume injection status"""
        base_status = super().format_status()
        
        if self._volume_injection_enabled:
            time_since_last = self._current_timestamp - self._last_transaction_timestamp
            status_lines = [
                "",
                "  Volume Injection Status:",
                f"    Enabled: Yes",
                f"    Min Transaction Interval: {self._min_transaction_interval}s",
                f"    Time Since Last Transaction: {time_since_last:.1f}s",
                f"    Injection Amount: {self._volume_injection_amount_pct}% of normal",
                f"    Injection Spread Tightening: {self._volume_injection_spread_pct}%"
            ]
            
            if self._volume_injection_cooldown > self._current_timestamp:
                cooldown_remaining = self._volume_injection_cooldown - self._current_timestamp
                status_lines.append(f"    Cooldown Remaining: {cooldown_remaining:.1f}s")
            
            base_status += "\n".join(status_lines)
            
        return base_status