import configparser
import datetime
from time import sleep
from botlogger import Logger
import sys
if '--Test' in sys.argv:
    from bybit_operations_backtrader import BybitOperations
else:
    from bybit_operations import BybitOperations


class BybitTools(BybitOperations):
    win = False
    last_big_deal = ''
    orders = []
    live = False

    def __init__(self):
        super(BybitTools, self).__init__()
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Boti Trading system initiated')

    def __destruct(self):
        self.logger.info('---------------------------------- End !!!!! ----------------------------------')

    def update_last_big_deal(self, symbol):
        new_big_deal = self.get_big_deal(symbol)
        if new_big_deal != self.last_big_deal and new_big_deal is not False:
            self.last_big_deal = new_big_deal

    def get_vwap(self, symbol):
        volume_array = []
        volume_close_array = []
        day_open = self.get_day_open()
        kline = self.get_kline(symbol, self.interval, day_open)
        for k in kline:
            volume_close_array.append((float(k["close"])+float(k["high"])+float(k["low"]))/3*float(k["volume"]))
            volume_array.append(float(k["volume"]))
        return int(sum(volume_close_array) / sum(volume_array)) + 2

    def wait_for_limit_order_fill(self, symbol, fill_thresh_hold):
        position = self.true_get_position(symbol)
        now = datetime.datetime.now()
        counter = 0
        while position['side'] == 'None' and counter < fill_thresh_hold:
            position = self.true_get_position(symbol)
            sleep(1)
            time_delta = datetime.datetime.now() - now
            counter = time_delta.seconds
        if counter >= fill_thresh_hold:
            self.logger.info("order did not met time constraints")
            self.logger.info("Canceling Limit Orders")
            self.logger.info(self.bybit.Order.Order_cancelAll(symbol=symbol).result())
            return False
        else:
            self.logger.info("Order accepted, Fill Time: {}".format(counter))
            return True

    def initiate_trade(self, symbol, quantity, side, targets, stop_px):
        self.logger.info('---------------------------------- New Trade ----------------------------------')
        position = self.true_get_position(symbol)
        self.logger.info("Current Trade, symbol: {} side: {} size: {} price: {}".format(
            symbol,
            self.get_position_side(position),
            self.get_position_size(position),
            self.get_position_price(position)
        ))
        position_price = self.get_position_price(position)
        quantity = int(quantity)
        self.orders.append(self.create_stop(symbol, stop_px))
        if side == 'Buy':
            opposite_side = 'Sell'
        else:
            opposite_side = 'Buy'
        for t in targets:
            if opposite_side == "Sell":
                t = t * position_price + position_price
            else:
                t = -t * position_price + position_price
            self.orders.append(self.limit_order(symbol, opposite_side, quantity/3, int(t)))

    def maintain_trade(self, symbol, stop, targets, quantity):
        quantity = int(quantity)
        position = self.true_get_position(symbol)
        position_size = self.get_position_size(position)
        stop_price = self.get_position_price(position)
        if abs(position_size) != quantity:
            self.win = True
            if abs(position_size) == int(self.config['OTHER']['Amount'])/3:
                position_side = self.get_position_side(position)
                if position_side == 'Sell':
                    stop_price = stop_price - targets[0] * stop_price
                else:
                    stop_price = stop_price + targets[0] * stop_price
            stop_price = str(int(stop_price))
            quantity = abs(position_size)
            self.logger.info("Amending stop as limit was filled, price:{} quantity:{}".format(stop_price, quantity))
            self.edit_stop(symbol, stop, quantity, stop_price)
        return quantity
