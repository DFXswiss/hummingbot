from hummingbot.connector.exchange_base cimport ExchangeBase
from .asset_price_delegate cimport AssetPriceDelegate

cdef class FixedAssetPriceDelegate(AssetPriceDelegate):
    cdef:
        ExchangeBase _market
        object _fixed_price
