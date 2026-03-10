import random
from decimal import Decimal
from typing import List, Optional

import aiohttp
from pydantic import Field

from hummingbot.core.data_type.common import TradeType
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
    price_source: str = Field(default="current_market", description="Price source: current_market | custom_api | fixed_price")
    price_source_custom_api: Optional[str] = Field(default=None, description="URL for custom API price source")
    custom_api_quote_key: str = Field(default="usd", description="JSON key to extract price from custom API response")
    price_source_fixed_price: Optional[Decimal] = Field(default=None, description="Fixed reference price")

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
            "last_candle": None,
        }

        self.market_data_provider.initialize_rate_sources([ConnectorPair(
            connector_name=config.trading_connector,
            trading_pair=config.trading_pair
        )])

    async def fetch_reference_price(self) -> Optional[Decimal]:
        if self.config.price_source == "fixed_price":
            return self.config.price_source_fixed_price
        if self.config.price_source == "custom_api":
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.config.price_source_custom_api) as resp:
                        data = await resp.json()
                        return Decimal(str(data[self.config.custom_api_quote_key]))
            except Exception as e:
                self.logger().error(f"Error fetching custom API price: {e}")
                return None
        return None

    async def update_processed_data(self):
        try:
            reference_price = await self.fetch_reference_price()
            if reference_price is None:
                self.logger().warning("Price source unavailable, skipping cycle")
            self.processed_data["last_bid"] = reference_price

            await self.fetch_candle_data()

        except Exception as e:
            self.logger().error(f"Error updating processed data: {e}")
            self.processed_data["last_bid"] = None

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

            active_order_executors = [e for e in self.executors_info if e.type == "order_executor" and e.is_active]

            if len(active_order_executors) > 0:
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
        price = self.processed_data.get("last_bid")

        if price is None or price <= 0:
            return []

        fraction = Decimal(str(random.uniform(0.75, 1.0)))
        amount = (self.config.order_amount_quote * fraction) / price
        timestamp = self.market_data_provider.time()
        actions: List[ExecutorAction] = []

        buy_config = OrderExecutorConfig(
            timestamp=timestamp,
            connector_name=self.config.trading_connector,
            trading_pair=self.config.trading_pair,
            side=TradeType.BUY,
            amount=amount,
            price=price,
            execution_strategy=ExecutionStrategy.LIMIT,
            controller_id=self.config.id,
        )
        sell_config = OrderExecutorConfig(
            timestamp=timestamp,
            connector_name=self.config.trading_connector,
            trading_pair=self.config.trading_pair,
            side=TradeType.SELL,
            amount=amount,
            price=price,
            execution_strategy=ExecutionStrategy.LIMIT,
            controller_id=self.config.id,
        )
        actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=buy_config))
        actions.append(CreateExecutorAction(controller_id=self.config.id, executor_config=sell_config))

        return actions

    def to_format_status(self):
        price = self.processed_data.get("last_bid")
        return [f"KeepMarketPMM price={price} q={self.config.order_amount_quote}"]

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
