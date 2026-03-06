from decimal import Decimal

from hummingbot.connector.exchange_base import ExchangeBase
from hummingbot.core.data_type.common import PriceType
from .asset_price_delegate cimport AssetPriceDelegate


cdef class FixedAssetPriceDelegate(AssetPriceDelegate):
    def __init__(self, market: ExchangeBase, fixed_price: Decimal):
        super().__init__()
        self._market = market
        self._fixed_price = fixed_price

    def get_price_by_type(self, _: PriceType) -> Decimal:
        return self.c_get_mid_price()

    cdef object c_get_mid_price(self):
        return self._fixed_price

    @property
    def ready(self) -> bool:
        return True

    @property
    def market(self) -> ExchangeBase:
        return self._market
