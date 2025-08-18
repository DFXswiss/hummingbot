import asyncio
import logging
from typing import Any, Dict, List, Optional

import numpy as np

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.data_feed.candles_feed.xt_spot_candles import constants as CONSTANTS
from hummingbot.logger import HummingbotLogger


class XtSpotCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"xt_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self):
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self):
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    async def _send_periodic_ping(self, ws: WSAssistant, interval: float = 10.0):
        """
        Periodically sends a raw string 'ping' message through the websocket connection.
        """
        try:
            while True:
                await asyncio.sleep(interval)
                await ws._connection._send_plain_text("ping")
        except asyncio.CancelledError:
            pass
        except Exception:
            self.logger().exception("Error sending periodic ping.")

    async def listen_for_subscriptions(self):
        """
        Connects to the candlestick websocket endpoint and listens to the messages sent by the
        exchange. Overrides the base class to add custom ping handling.
        """
        ws: Optional[WSAssistant] = None
        ping_task: Optional[asyncio.Task] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                ping_task = asyncio.create_task(self._send_periodic_ping(ws))
                await self._subscribe_channels(ws)
                await self._process_websocket_messages_task(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except ConnectionError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when listening to public klines. Retrying in 1 seconds...",
                )
                await self._sleep(3.0)
            finally:
                if ping_task is not None:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except Exception:
                        pass
                await self._on_order_stream_interruption(websocket_assistant=ws)

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(url=self.health_check_url,
                                             throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair):
        return trading_pair.replace("-", "_").lower()

    @property
    def _is_first_candle_not_included_in_rest_request(self):
        return False

    @property
    def _is_last_candle_not_included_in_rest_request(self):
        return False

    def _get_rest_candles_params(self,
                                 start_time: Optional[int] = None,
                                 end_time: Optional[int] = None,
                                 limit: Optional[int] = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST) -> dict:
        """
        For API documentation, please refer to:
        https://doc.xt.com/#rest-publickline

        startTime and endTime must be used at the same time.
        """
        params = {
            "symbol": self._ex_trading_pair,
            "interval": CONSTANTS.INTERVALS[self.interval],
            "limit": limit
        }
        if start_time is not None:
            params["startTime"] = start_time * 1000
        if end_time is not None:
            params["endTime"] = end_time * 1000

        return params

    def _parse_rest_candles(self, data: dict, end_time: Optional[int] = None) -> List[List[float]]:
        if data is not None and data.get("result") is not None:
            candles = data["result"]
            if candles is not None and len(candles) > 0:
                parsed_candles = []
                for i, row in enumerate(candles):
                    try:
                        timestamp = self.ensure_timestamp_in_seconds(row["t"])
                        open_price = float(row["o"])
                        close_price = float(row["c"])
                        high_price = float(row["h"])
                        low_price = float(row["l"])
                        quote_volume = float(row["q"])
                        base_volume = float(row["v"])

                        parsed_candles.append([timestamp, open_price, high_price, low_price, close_price, base_volume, quote_volume, 0., 0., 0.])
                    except (KeyError, IndexError, ValueError) as e:
                        self.logger().warning(f"Error parsing candle row {i}: {e} - {row}")
                        continue

                return parsed_candles
            else:
                self.logger().debug("No historical candles data available from REST API")
        else:
            self.logger().warning(f"Unexpected REST API response format: {data}")

        return []

    def ws_subscription_payload(self):
        trading_pair = self.get_exchange_trading_pair(self._trading_pair)
        candle_params = [f"kline@{trading_pair.lower()},{self.interval}"]
        payload = {
            "method": "subscribe",
            "params": candle_params,
            "id": 1
        }
        return payload

    async def fill_historical_candles(self):
        """
        This method fills the historical candles in the _candles deque until it reaches the maximum length.
        Overrides the base class to handle empty deque by fetching initial historical data.
        """
        while not self.ready:
            await self._ws_candle_available.wait()
            try:
                if len(self._candles) == 0:
                    current_time = int(self._time())
                    candles: np.ndarray = await self.fetch_candles(end_time=current_time, limit=self._candles.maxlen)
                    if len(candles) > 0:
                        candles = candles[candles[:, 0].argsort()]
                        for candle in candles:
                            self._candles.append(candle)
                        continue
                    else:
                        await self._sleep(1.0)
                        continue

                end_time = self._round_timestamp_to_interval_multiple(self._candles[0][0])
                missing_records = self._candles.maxlen - len(self._candles)
                candles: np.ndarray = await self.fetch_candles(end_time=end_time, limit=missing_records)

                if candles is None or len(candles) == 0:
                    self.logger().debug("No candles returned from fetch_candles")
                    await self._sleep(1.0)
                    continue

                if not isinstance(candles, np.ndarray):
                    candles = np.array(candles)

                if len(candles) > 0:
                    candles = candles[candles[:, 0] < end_time]

                if len(candles) > 0:
                    candles = candles[candles[:, 0].argsort()]

                records_to_add = min(missing_records, len(candles))
                if records_to_add > 0:
                    self._candles.extendleft(candles[:records_to_add][::-1])
            except asyncio.CancelledError:
                raise
            except ValueError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error occurred when getting historical klines. Retrying in 1 seconds...",
                )
                await self._sleep(1.0)
        self.check_candles_sorted_and_equidistant(np.array(self._candles))

    def _parse_websocket_message(self, data):
        candles_row_dict: Dict[str, Any] = {}
        # Handle ping/pong responses - they come as strings
        if isinstance(data, str):
            if data == "pong":
                return None
        if data is not None and data.get("data") is not None and data.get("topic") is not None:
            topic = data["topic"]
            if "kline" in topic:
                candle = data["data"]
                candles_row_dict["timestamp"] = self.ensure_timestamp_in_seconds(candle["t"])
                candles_row_dict["open"] = candle["o"]
                candles_row_dict["low"] = candle["l"]
                candles_row_dict["high"] = candle["h"]
                candles_row_dict["close"] = candle["c"]
                candles_row_dict["volume"] = candle["v"]
                candles_row_dict["quote_asset_volume"] = 0.
                candles_row_dict["n_trades"] = 0.
                candles_row_dict["taker_buy_base_volume"] = 0.
                candles_row_dict["taker_buy_quote_volume"] = 0.
                return candles_row_dict
        return None
