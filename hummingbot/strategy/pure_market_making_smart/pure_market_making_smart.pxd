# distutils: language=c++

from hummingbot.strategy.pure_market_making.pure_market_making cimport PureMarketMakingStrategy

cdef class PureMarketMakingSmartStrategy(PureMarketMakingStrategy):
    pass