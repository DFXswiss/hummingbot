from decimal import Decimal
from hummingbot.client.config.config_validators import validate_bool, validate_decimal
from hummingbot.client.config.config_var import ConfigVar
from hummingbot.strategy.pure_market_making.pure_market_making_config_map import pure_market_making_config_map

# Inherit all config from pure_market_making and add volume support configs
pure_market_making_volume_support_config_map = pure_market_making_config_map.copy()

# Add volume injection specific configurations
pure_market_making_volume_support_config_map.update({
    "volume_injection_enabled":
        ConfigVar(key="volume_injection_enabled",
                  prompt="Do you want to enable volume injection to ensure minimum transaction frequency? (Yes/No) >>> ",
                  type_str="bool",
                  default=False,
                  validator=validate_bool),
    "min_transaction_interval":
        ConfigVar(key="min_transaction_interval",
                  prompt="What is the maximum time in seconds between transactions before volume injection activates? >>> ",
                  required_if=lambda: pure_market_making_volume_support_config_map.get("volume_injection_enabled").value,
                  type_str="float",
                  default=60.0,
                  validator=lambda v: validate_decimal(v, min_value=10, max_value=300)),
    "volume_injection_amount_pct":
        ConfigVar(key="volume_injection_amount_pct",
                  prompt="What percentage of normal order_amount should volume injection orders use? (Enter 10 for 10%) >>> ",
                  required_if=lambda: pure_market_making_volume_support_config_map.get("volume_injection_enabled").value,
                  type_str="decimal",
                  default=Decimal("15"),
                  validator=lambda v: validate_decimal(v, min_value=1, max_value=100)),
    "volume_injection_spread_pct":
        ConfigVar(key="volume_injection_spread_pct",
                  prompt="What percentage tighter than normal spread should volume injection orders use? (Enter 20 for 20% tighter) >>> ",
                  required_if=lambda: pure_market_making_volume_support_config_map.get("volume_injection_enabled").value,
                  type_str="decimal",
                  default=Decimal("20"),
                  validator=lambda v: validate_decimal(v, min_value=0, max_value=50))
})