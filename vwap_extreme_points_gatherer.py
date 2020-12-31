from botlogger import Logger
import datetime
from time import sleep
import sys
import pickle
from bybit_tools import BybitTools


class VwapExtremePointsGatherer(BybitTools):
    price_above = False
    extreme_points_above_vwap = []
    extreme_points_below_vwap = []
    points_dict = {}

    def __init__(self):
        super(VwapExtremePointsGatherer, self).__init__()
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Applying Vwap Strategy')

    def has_price_crossed_vwap(self, last_price, vwap):
        if last_price < vwap and self.price_above and len(self.points_dict) != 0:  # Price just crossed vwap to the downside
            self.extreme_points_above_vwap.append(max(self.points_dict.items(), key=lambda x: x[1]))
            self.extreme_points_above_vwap.sort(key=lambda tup: tup[1])
            self.points_dict = {}
            self.logger.info("Price just crossed vwap to the downside {}".format(self.get_date))
            return True
        if last_price > vwap and not self.price_above and len(self.points_dict) != 0:  # Price just crossed vwap to the upside
            self.extreme_points_below_vwap.append(max(self.points_dict.items(), key=lambda x: x[1]))
            self.extreme_points_below_vwap.sort(key=lambda tup: tup[1])
            self.points_dict = {}
            self.logger.info("Price just crossed vwap to the upside {}".format(self.get_date))
            return True
        return False

    def strategy_run(self, symbol, last_price, vwap):
        res = self.has_price_crossed_vwap(last_price, vwap)
        if res:
            print("Extreme points below vwap: {} Extreme points above vwap: {}".format(
                self.extreme_points_below_vwap, self.extreme_points_above_vwap)
            )
            with open('extreme_points_below_vwap', 'wb') as ep:
                pickle.dump(self.extreme_points_below_vwap, ep)
            with open('extreme_points_above_vwap', 'wb') as ep:
                pickle.dump(self.extreme_points_above_vwap, ep)

        kline = self.get_last_kline(symbol, self.interval)
        if last_price > vwap:  # This determines if price is above or below vwap
            self.price_above = True
            self.points_dict[kline['timestamp']] = ((kline['high'] / vwap) - 1) * 100
        else:
            self.price_above = False
            self.points_dict[kline['timestamp']] = ((vwap / kline['low']) - 1) * 100

    def next(self):
        symbol = "BTCUSD"
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        self.strategy_run(symbol, last_price, vwap)
        while self.live:
            if datetime.datetime.now().second % 10 != 0:
                sleep(1)
                continue
            vwap = self.get_vwap(symbol)
            last_price = self.get_last_price_close(symbol)
            self.strategy_run(symbol, last_price, vwap)
