from decimal import Decimal
from typing import List

from pydantic import Field

from hummingbot.core.data_type.common import PriceType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction, StopExecutorAction


class KeepMarketPMMConfig(ControllerConfigBase):
    controller_name: str = "keep_market_pmm"
    trading_connector: str = Field(default="xt", description="Connector where orders will be placed")
    trading_pair: str = Field(default="DEPS-USDT", description="Pair to trade")
    order_amount_quote: Decimal = Field(default=Decimal("100"), description="Quote amount per order")
    update_interval: float = Field(default=8.0, description="Update interval in seconds")
    candles_config: List[CandlesConfig] = []

    def update_markets(self, markets):
        return markets.add_or_update(self.trading_connector, self.trading_pair)


class KeepMarketPMM(ControllerBase):
    def __init__(self, config: KeepMarketPMMConfig, market_data_provider=None, actions_queue=None, update_interval=None, *args, **kwargs):
        if len(config.candles_config) == 0:
            config.candles_config = [CandlesConfig(
                connector=config.trading_connector,
                trading_pair=config.trading_pair,
                interval="1m",
                max_records=10
            )]

        if update_interval is None:
            update_interval = config.update_interval

        super().__init__(config, market_data_provider, actions_queue, update_interval)
        self.config: KeepMarketPMMConfig = config

        self.processed_data = {
            "last_bid": None,
            "last_ask": None,
            "last_candle": None,
        }

        self.is_buy_cycle = True

        self.market_data_provider.initialize_rate_sources([ConnectorPair(
            connector_name=config.trading_connector,
            trading_pair=config.trading_pair
        )])

    async def update_processed_data(self):
        try:
            bid = self.market_data_provider.get_price_by_type(
                self.config.trading_connector,
                self.config.trading_pair,
                PriceType.BestBid
            )
            ask = self.market_data_provider.get_price_by_type(
                self.config.trading_connector,
                self.config.trading_pair,
                PriceType.BestAsk
            )

            await self.fetch_candle_data()

            if bid is not None and bid != 0:
                self.processed_data["last_bid"] = Decimal(str(bid))
            else:
                self.logger().warning(f"Invalid bid price: {bid}")
                self.processed_data["last_bid"] = None

            if ask is not None and ask != 0:
                self.processed_data["last_ask"] = Decimal(str(ask))
            else:
                self.logger().warning(f"Invalid ask price: {ask}")
                self.processed_data["last_ask"] = None

        except Exception as e:
            self.logger().error(f"Error getting bid/ask prices: {e}")
            self.processed_data["last_bid"] = None
            self.processed_data["last_ask"] = None

    async def fetch_candle_data(self):
        try:
            if len(self.config.candles_config) > 0:
                candles_config = self.config.candles_config[0]

                candles_df = self.market_data_provider.get_candles_df(
                    connector_name=candles_config.connector,
                    trading_pair=candles_config.trading_pair,
                    interval=candles_config.interval,
                    max_records=candles_config.max_records
                )

                if candles_df is not None and not candles_df.empty:
                    latest_candle = candles_df.iloc[-1]
                    self.processed_data["last_candle"] = latest_candle
                else:
                    self.processed_data["last_candle"] = None

        except Exception as e:
            self.logger().error(f"Error fetching candle data: {e}")

    def determine_executor_actions(self) -> List[ExecutorAction]:
        actions: List[ExecutorAction] = []

        try:
            active_executors = [e for e in self.executors_info if e.is_active]
            current_time = self.market_data_provider.time()

            if len(active_executors) > 0:
                for executor in active_executors:
                    if current_time - executor.timestamp > 20.0:
                        stop_action = StopExecutorAction(
                            controller_id=self.config.id,
                            executor_id=executor.id,
                            keep_position=False
                        )
                        actions.append(stop_action)

            self.logger().info(f"----> {self.config.trading_pair} tick")

            # Filter only order executors for the time check
            order_executors = [e for e in self.executors_info if e.type == "order_executor"]

            if len(order_executors) > 0:
                latest_executor = max(order_executors, key=lambda x: x.timestamp)
                time_since_last_trade = current_time - latest_executor.timestamp

                if time_since_last_trade < 50.0:
                    return actions

            last_candle = self.processed_data.get("last_candle")
            self.logger().info(f"----> {self.config.trading_pair} In range time to send order")

            # Handle missing or invalid candle data
            if last_candle is None or not isinstance(last_candle.get('timestamp'), (int, float)):
                place_order_actions = self.get_order_actions()
                actions.extend(place_order_actions)
                self.logger().info(f"----> {self.config.trading_pair} no last candle, place order")
                return actions

            # No activity for 2 minutes, place order
            if last_candle['timestamp'] + 120 < current_time:
                self.logger().info(f"----> {self.config.trading_pair} no activity for 2 minutes, place order")
                place_order_actions = self.get_order_actions()
                actions.extend(place_order_actions)
                return actions

            close_last_candle_time = last_candle['timestamp'] + 60

            # If the candle is open, return early
            if current_time <= close_last_candle_time:
                self.logger().info(f"----> {self.config.trading_pair} there is activity in this period, skipping....")
                self.logger().info(f"----> {self.config.trading_pair} current time: {current_time}, close_last_candle_time: {close_last_candle_time}")
                self.logger().info(f"----> {self.config.trading_pair} last candle: {last_candle}")
                return actions

            # If there is a candle for the previous interval, but not for the current interval
            if current_time > close_last_candle_time and current_time < close_last_candle_time + 60 and current_time + 10 > close_last_candle_time + 60:
                place_order_actions = self.get_order_actions()
                actions.extend(place_order_actions)
                self.logger().info(f"----> {self.config.trading_pair} candle about to close without activity, place order")
                return actions

        except Exception as e:
            self.logger().error(f"Error in determine_executor_actions: {e}")

        return actions

    def get_order_actions(self):
        bid = self.processed_data.get("last_bid")
        ask = self.processed_data.get("last_ask")

        if bid is None or ask is None:
            return []

        if bid <= 0 or ask <= 0:
            return []

        actions: List[ExecutorAction] = []

        if self.is_buy_cycle:
            buy_amount = self.config.order_amount_quote / ask

            buy_config = OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.trading_connector,
                trading_pair=self.config.trading_pair,
                side=TradeType.BUY,
                amount=buy_amount,
                price=ask,
                execution_strategy=ExecutionStrategy.LIMIT,
                controller_id=self.config.id,
            )
            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=buy_config))
        else:
            sell_amount = self.config.order_amount_quote / bid

            sell_config = OrderExecutorConfig(
                timestamp=self.market_data_provider.time(),
                connector_name=self.config.trading_connector,
                trading_pair=self.config.trading_pair,
                side=TradeType.SELL,
                amount=sell_amount,
                price=bid,
                execution_strategy=ExecutionStrategy.LIMIT,
                controller_id=self.config.id,
            )
            actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=sell_config))

        # Randomly select next cycle (buy or sell)
        self.is_buy_cycle = not self.is_buy_cycle

        return actions

    def to_format_status(self):
        bid = self.processed_data.get("last_bid")
        ask = self.processed_data.get("last_ask")
        next_order_type = "BUY" if self.is_buy_cycle else "SELL"
        return [f"KeepMarketPMM bid={bid} ask={ask} q={self.config.order_amount_quote} next={next_order_type}"]

    async def control_task(self):
        connector_ready = any(connector.ready for connector in self.market_data_provider.connectors.values())

        if connector_ready and self.executors_update_event.is_set():
            await self.update_processed_data()
            executor_actions: List[ExecutorAction] = self.determine_executor_actions()
            if len(executor_actions) > 0:
                await self.send_actions(executor_actions)
            else:
                self.executors_update_event.set()
        else:
            if not self.executors_update_event.is_set():
                self.executors_update_event.set()
