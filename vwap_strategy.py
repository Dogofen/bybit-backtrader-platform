from botlogger import Logger
import datetime
from time import sleep
from bybit_tools import BybitTools


class VwapStrategy(BybitTools):
    symbol = "BTCUSD"
    coin = "BTC"
    cash = False
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

    def __init__(self):
        super(VwapStrategy, self).__init__()
        self.targets = [
            float(self.config["Vwap"]["Target0"]),
            float(self.config["Vwap"]["Target1"]),
            float(self.config["Vwap"]["Target2"])
        ]
        self.stop_px = self.config["Vwap"]["StopPx"]
        self.wait_time_limit = int(self.config["Vwap"]["WaitTimeLimit"])
        self.amount = self.config["OTHER"]["Amount"]
        self.draw_back_percentage = float(self.config["OTHER"]["DrawBack"])
        self.cash = self.get_cash(self.coin)
        self.draw_back = self.cash - self.cash * self.draw_back_percentage
        self.logger.info("Initial cash: {} max draw back: {}, {}".format(
            self.cash, self.draw_back_percentage, self.draw_back)
        )
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Applying Vwap Strategy')

    def finish_operations_for_trade(self, symbol):
        print("Trade finished, win: {} time:{}".format(self.win, self.get_date()))
        self.logger.info("Trade finished, win: {} time {}".format(
            self.win, self.get_date())
        )
        if not self.win:
            self._wait = True
        self.cancel_all_orders(symbol)
        self.in_a_trade = False
        self.amount = self.config["OTHER"]["Amount"]
        self.win = False
        self.logger.info('Remaining open orders: {} stop orders: {}'.format(
            self.true_get_active_orders(symbol), self.true_get_stop_order(symbol)))
        self.logger.info('---------------------------------- End ----------------------------------')
        cash = self.get_cash(self.coin)
        if cash < self.draw_back:  # quit if bot lose too much
            self.logger.error(
                "Asset: {} has passed the max draw back defined: {} thus stopping.".format(cash, self.draw_back)
            )
            quit()

    def in_trade_operations(self, symbol):
        stop = self.get_stop_order()
        self.amount = self.maintain_trade(symbol, stop, self.targets, self.amount)

    def start_trade(self, symbol, position):
        print("Trade started time:{}".format(self.get_date()))
        self.in_a_trade = True
        if len(self.orders) == 1:
            self.orders.pop()
            side = self.get_position_side(position)
            self.initiate_trade(symbol, self.amount, side, self.targets, self.stop_px + '%')

    def adjust_order_to_vwap(self, symbol, vwap):
        if self.last_vwap != vwap and vwap:
            self.edit_orders_price(symbol, self.orders[0], vwap)
        return vwap

    def put_limit_order(self, symbol, vwap, last_price):
        if last_price > vwap:
            self.orders.append(self.limit_order(symbol, "Buy", self.amount, vwap))
        else:
            self.orders.append(self.limit_order(symbol, "Sell", self.amount, vwap))

    def wait(self, last_price, vwap):
        self.logger.info("Trading has stopped and now in wait time, wait: {}".format(self.wait_time))
        if last_price < vwap and self.price_above:  # Price just crossed vwap
            self.wait_time = 0
            self.logger.info("Zeroing wait time as price crossed vwap {}".format(self.wait_time))
        if last_price > vwap and not self.price_above:  # Price just crossed vwap
            self.wait_time = 0
            self.logger.info("Zeroing wait time as price crossed vwap {}".format(self.wait_time))
        self.wait_time += 1
        if self.wait_time == self.wait_time_limit:
            self._wait = False
            self.wait_time = 0

    def strategy_run(self, symbol, position, last_price, vwap):
        position_size = self.get_position_size(position)
        # waiting when day passed if no trade
        if datetime.datetime.now().strftime('%H:%M:%S') == self.get_time_open() and not self.in_a_trade:
            self.logger.info("Waiting a few minutes at the start of day open")
            self._wait = True
            self.cancel_all_orders(symbol)

        if position_size == 0 and self.in_a_trade:  # Finish Operations
            self.finish_operations_for_trade(symbol)

        if position_size != 0 and self.in_a_trade:  # When in Trade, maintaining
            self.in_trade_operations(symbol)

        if position_size != 0 and not self.in_a_trade:  # When limit order just accepted
            self.start_trade(symbol, position)

        if not self.in_a_trade and len(self.orders) == 1:  # Editing Order every tick to fit vwap
            self.last_vwap = self.adjust_order_to_vwap(symbol, vwap)

        if not self.in_a_trade and len(self.orders) == 0 and not self._wait:  # Send First Limit order
            self.put_limit_order(symbol, vwap, last_price)

        if self._wait:  # If waiting is needed between trades
            self.wait(last_price, vwap)

        if last_price > vwap:  # This determines if price has crossed vwap
            self.price_above = True
        else:
            self.price_above = False

    def next(self):
        vwap = self.get_vwap(self.symbol)
        last_price = self.get_last_price_close(self.symbol)
        position = self.true_get_position(self.symbol)
        self.strategy_run(self.symbol, position, last_price, vwap)

        while self.live:
            if datetime.datetime.now().second % 10 != 0:
                sleep(1)
                continue
            vwap = self.get_vwap(self.symbol)
            last_price = self.get_last_price_close(self.symbol)
            position = self.true_get_position(self.symbol)
            self.strategy_run(self.symbol, position, last_price, vwap)
