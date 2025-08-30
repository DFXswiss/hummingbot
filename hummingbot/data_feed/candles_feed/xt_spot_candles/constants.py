from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

REST_URL = "https://sapi.xt.com"
HEALTH_CHECK_ENDPOINT = "/v4/public/time"
CANDLES_ENDPOINT = "/v4/public/kline"

WSS_URL = "wss://stream.xt.com/public"

INTERVALS = bidict({
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d"
})

MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

RATE_LIMITS = [
    RateLimit(CANDLES_ENDPOINT, limit=10, time_interval=1, linked_limits=[LinkedLimitWeightPair("raw", 1)]),
    RateLimit(HEALTH_CHECK_ENDPOINT, limit=10, time_interval=1, linked_limits=[LinkedLimitWeightPair("raw", 1)])
]
