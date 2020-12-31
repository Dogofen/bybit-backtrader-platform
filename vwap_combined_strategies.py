from botlogger import Logger
import datetime
from time import sleep
from bybit_tools import BybitTools
import sys
from vwap_strategy import VwapStrategy
from vwap_extreme_points_strategy import VwapExtremePointsStrategy


class VwapCombinedStrategy(BybitTools):
    vwap_strategy = False
    vwap_extreme_points_strategy = False

    def __init__(self):
        super(VwapCombinedStrategy, self).__init__()
        self.vwap_strategy = VwapStrategy
        self.vwap_extreme_points_strategy = VwapExtremePointsStrategy
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

    def next(self):
        symbol = "BTCUSD"
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        position = self.true_get_position(symbol)
        self.vwap_strategy.strategy_run(self, symbol, position, last_price, vwap)
        self.vwap_extreme_points_strategy.strategy_run(self, symbol, position, last_price, vwap)
        while self.live:
            if datetime.datetime.now().second % 10 != 0:
                sleep(1)
                continue
            vwap = self.get_vwap(symbol)
            last_price = self.get_last_price_close(symbol)
            position = self.true_get_position(symbol)
            self.vwap_strategy.strategy_run(symbol, position, last_price, vwap)
            self.vwap_extreme_points_strategy.strategy_run(symbol, position, last_price, vwap)
