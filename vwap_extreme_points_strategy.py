from botlogger import Logger
import datetime
from time import sleep
from bybit_tools import BybitTools
import configparser
import sys


class VwapExtremePointsStrategy(BybitTools):
    old_position = False
    last_big_deal = False
    new_big_deal = False
    targets = []
    stop_px = False
    amount = False
    fill_thresh_hold = 1
    in_a_trade = False
    win = False
    _wait = False
    wait_time = 0
    wait_time_limit = False
    price_above = False
    last_vwap = False
    long_entries = []
    short_entries = []
    entry_counter = 0

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        self.targets = [
            [
                float(self.config["VwapExtremePoints"]["Target0"]),
                float(self.config["VwapExtremePoints"]["Target1"]),
                float(self.config["VwapExtremePoints"]["Target2"])
             ],
            [
                float(self.config["VwapExtremePoints"]["Target3"]),
                float(self.config["VwapExtremePoints"]["Target4"]),
                float(self.config["VwapExtremePoints"]["Target5"])
            ],
            [
                float(self.config["VwapExtremePoints"]["Target6"]),
                float(self.config["VwapExtremePoints"]["Target7"]),
                float(self.config["VwapExtremePoints"]["Target8"])
                ],
            [
                float(self.config["VwapExtremePoints"]["Target9"]),
                float(self.config["VwapExtremePoints"]["Target10"]),
                float(self.config["VwapExtremePoints"]["Target11"])
            ]
        ]
        self.long_entries = [
            self.config['VwapExtremePoints']['LongEntry0'],
            self.config['VwapExtremePoints']['LongEntry1'],
            self.config['VwapExtremePoints']['LongEntry2'],
            self.config['VwapExtremePoints']['LongEntry3']
            ]
        self.short_entries = [
            self.config['VwapExtremePoints']['ShortEntry0'],
            self.config['VwapExtremePoints']['ShortEntry1'],
            self.config['VwapExtremePoints']['ShortEntry2'],
            self.config['VwapExtremePoints']['ShortEntry3']
            ]
        self.stop_px = [
            self.config["VwapExtremePoints"]["StopPx0"],
            self.config["VwapExtremePoints"]["StopPx1"],
            self.config["VwapExtremePoints"]["StopPx2"],
            self.config["VwapExtremePoints"]["StopPx3"]
            ]
        self.wait_time_limit = int(self.config["VwapExtremePoints"]["WaitTimeLimit"])
        self.amount = self.config["OTHER"]["Amount"]
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True
        super(VwapExtremePointsStrategy, self).__init__()
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Applying Vwap Strategy')

    def finish_operations_for_trade(self, symbol):
        print("Trade finished, win: {} time:{}".format(self.win, self.get_date()))
        self.logger.info("Trade finished, win: {} time {}".format(
            self.win, self.get_date())
        )
        if self.entry_counter < len(self.long_entries) - 1:
            self.entry_counter += 1
        self.cancel_all_orders(symbol)
        self.in_a_trade = False
        self.amount = self.config["OTHER"]["Amount"]
        self.win = False
        self.logger.info('---------------------------------- End ----------------------------------')

    def in_trade_operations(self, symbol):
        stop = self.get_stop_order()
        self.amount = self.maintain_trade(symbol, stop, self.targets[self.entry_counter], self.amount)

    def start_trade(self, symbol, position):
        print("Trade started time:{}".format(self.get_date()))
        self.in_a_trade = True
        if len(self.orders) == 1:
            self.orders.pop()
            side = self.get_position_side(position)
            self.initiate_trade(
                symbol,
                self.amount,
                side,
                self.targets[self.entry_counter],
                self.stop_px[self.entry_counter] + '%'
            )

    def adjust_order_to_vwap(self, symbol, vwap):
        if self.last_vwap != vwap and vwap:
            self.edit_orders_price(symbol, self.orders[0], vwap)
        return vwap

    def put_limit_order(self, symbol, vwap, last_price):
        if last_price > vwap:
            if ((last_price / vwap) - 1) * 100 > float(self.short_entries[self.entry_counter]):
                self.logger.info(
                    "Creating Limit order with side: Sell and entry: {}".format(self.short_entries[self.entry_counter])
                )
                self.orders.append(self.limit_order(symbol, "Sell", self.amount, last_price))
        else:
            if ((vwap / last_price) - 1) * 100 > float(self.long_entries[self.entry_counter]):
                self.logger.info(
                    "Creating Limit order with side: Buy and entry: {}".format(self.long_entries[self.entry_counter])
                )
                self.orders.append(self.limit_order(symbol, "Buy", self.amount, last_price))

    def zero_entry_counter(self, last_price, vwap):
        if last_price < vwap and self.price_above and self.entry_counter != 0:  # Price just crossed vwap
            self.entry_counter = 0
            self.logger.info("Zeroing entry counter as price crossed vwap {}".format(self.wait_time))
        if last_price > vwap and not self.price_above and self.entry_counter != 0:  # Price just crossed vwap
            self.entry_counter = 0
            self.logger.info("Zeroing entry counter as price crossed vwap {}".format(self.wait_time))

    def price_above_below_vwap(self, last_price, vwap):
        if last_price > vwap:  # This determines if price has crossed vwap
            self.price_above = True
        else:
            self.price_above = False

    def strategy_run(self, symbol, position, last_price, vwap):
        position_size = self.get_position_size(position)

        if position_size == 0 and self.in_a_trade:  # Finish Operations
            self.finish_operations_for_trade(symbol)

        if position_size != 0 and self.in_a_trade:  # When in Trade, maintaining
            self.in_trade_operations(symbol)

        if position_size != 0 and not self.in_a_trade:  # When limit order just accepted
            self.start_trade(symbol, position)

        if not self.in_a_trade and len(self.orders) == 0:  # Send First Limit order
            self.put_limit_order(symbol, vwap, last_price)

        if not self.in_a_trade:
            self.zero_entry_counter(last_price, vwap)  # If zeroing entry counter is needed between trades
            self.price_above_below_vwap(last_price, vwap)  # determines if price has crossed vwap

    def next(self):
        symbol = "BTCUSD"
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        position = self.true_get_position(symbol)
        self.strategy_run(symbol, position, last_price, vwap)
        while self.live:
            if datetime.datetime.now().second % 10 != 0:
                sleep(1)
                continue
            vwap = self.get_vwap(symbol)
            last_price = self.get_last_price_close(symbol)
            position = self.true_get_position(symbol)
            self.strategy_run(symbol, position, last_price, vwap)
