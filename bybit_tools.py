import configparser
import datetime
import numpy as np
from time import sleep
from botlogger import Logger
import sys
if '--Test' in sys.argv:
    from backtrader_operations import BybitOperations
else:
    from bybit_operations import BybitOperations


class BybitTools(BybitOperations):
    win = False
    last_big_deal = ''
    orders = []
    live = False
    sell_spike_factor = 0
    buy_spike_factor = 0
    minimum_liquidations = 0
    liquidations_dict = {}
    liquidations_buy_thresh_hold = 0
    liquidations_sell_thresh_hold = 0

    def __init__(self):
        super(BybitTools, self).__init__()
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

        self.sell_spike_factor = 3
        self.buy_spike_factor = 5
        self.liquidations_buy_thresh_hold = 255140
        self.liquidations_sell_thresh_hold = 951181
        self.minimum_liquidations = 900
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Boti Trading system initiated')

    def __destruct(self):
        self.logger.info('---------------------------------- End !!!!! ----------------------------------')

    def update_last_big_deal(self, symbol):
        new_big_deal = self.get_big_deal(symbol)
        if new_big_deal != self.last_big_deal and new_big_deal is not False:
            self.last_big_deal = new_big_deal

    def is_long_position(self, symbol):
        position = self.true_get_position(symbol)
        side = self.get_position_side(position)
        if side == "Buy":
            return "Long"
        else:
            return "Short"

    def get_last_liquidations(self, liqs, days_interval):
        index_element = False
        for d in liqs:
            _time = datetime.datetime.fromtimestamp(int(d['time'] / 1000))
            if _time < self.get_datetime() - datetime.timedelta(days=days_interval):
                index_element = d
                break
        index = liqs.index(index_element)
        return liqs[index:]

    def get_vwap(self, symbol):
        volume_array = []
        volume_close_array = []
        day_open = self.get_day_open()
        kline = self.get_kline(symbol, self.interval, day_open)
        for k in kline:
            volume_close_array.append((float(k["close"])+float(k["high"])+float(k["low"]))/3*float(k["volume"]))
            volume_array.append(float(k["volume"]))
        return int(sum(volume_close_array) / sum(volume_array)) + 2

    def update_buy_sell_thresh_hold(self, liquidation_list):
        liq_1m_dict = {}
        sell_array = []
        buy_array = []
        for x in liquidation_list:
            time = datetime.datetime.fromtimestamp(int(x['time'] / 1000)).strftime("%d/%m/%Y, %H:%M")
            if time not in liq_1m_dict.keys():
                liq_1m_dict[time] = {"Buy": 0, "Sell": 0}
            liq_1m_dict[time][x['side']] += x['qty']
        for k in liq_1m_dict.keys():
            sell_array.append(liq_1m_dict[k]['Sell'])
            buy_array.append(liq_1m_dict[k]['Buy'])

        sell_array.sort()
        buy_array.sort()
        sell_array.reverse()
        buy_array.reverse()
        element = 0
        while element == 0 and len(sell_array) > 0:
            element = sell_array.pop()
        element = 0
        while element == 0 and len(buy_array) > 0:
            element = buy_array.pop()
        if len(buy_array) > 3:
            self.liquidations_buy_thresh_hold = buy_array[len(buy_array) - int(len(buy_array) * 0.8)]
        if len(sell_array) > 3:
            self.liquidations_sell_thresh_hold = sell_array[len(sell_array) - int(len(sell_array) * 0.8)]

    def update_liquidation_dict(self, symbol):
        _now = datetime.datetime.strptime(self.liq_current_time_no_seconds(), '%d/%m/%Y, %H:%M')
        from_time_in_minutes = (_now - datetime.timedelta(seconds=600))
        self.liquidations_dict = self.get_current_liquidations_dict(symbol, from_time_in_minutes)

    def return_datetime_from_liq_dict(self, value, side):
        for k in self.liquidations_dict.keys():
            if self.liquidations_dict[k][side] == value:
                return datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M')
            return False

    def get_liquidations_signal(self, side):
        sell_array = []
        buy_array = []
        for k in self.liquidations_dict.keys():
            sell_array.append(self.liquidations_dict[k]["Sell"])
            buy_array.append(self.liquidations_dict[k]["Buy"])
        if side is "Buy":
            if len(buy_array) < 3:
                return False
            buy_array.reverse()
            ba = np.diff(buy_array)
            th = self.liquidations_buy_thresh_hold
            if buy_array[-1] > self.buy_spike_factor * self.liquidations_buy_thresh_hold:
                if (self.get_datetime() - self.return_datetime_from_liq_dict(buy_array[-1], "Buy")).seconds == 60:
                    self.logger.info("Returning Buy signal based on big Buy liquidations spike")
                    return True
            if ba[-1] < -self.sell_spike_factor*th and ba[-2] > self.sell_spike_factor*th:
                if buy_array[-1]/buy_array[-2] < 0.3 and buy_array[-3]/buy_array[-2] < 0.3:
                    if buy_array[-1] > self.minimum_liquidations and buy_array[-3] > self.minimum_liquidations:
                        self.logger.info("Returning Buy signal based on 'Spiky hill' Buy liquidations pattern")
                        return True
            if len(buy_array) > 3:
                th = self.liquidations_buy_thresh_hold
                if ba[-1] < -th and ba[-2] > th and ba[-3] < -th:
                    self.logger.info("Returning Sell signal based on 'downhill' Buy liquidations pattern")
                    return True
                elif ba[-2] > 0 and ba[-1] > 0:
                    if (self.get_datetime() - self.return_datetime_from_liq_dict(buy_array[-1], "Buy")).seconds == 60:
                        self.logger.info("Returning Buy signal based on 'cliff' liquidations pattern")
                        return True
            #return ba[-2] > 0 > ba[-1] and buy_array[-2] > self.liquidations_buy_thresh_hold
            return False
        if side is "Sell":
            if len(sell_array) < 3:
                return False
            sell_array.reverse()
            sa = np.diff(sell_array)
            th = self.liquidations_sell_thresh_hold
            if sell_array[-1] > self.sell_spike_factor * self.liquidations_sell_thresh_hold:
                if (self.get_datetime() - self.return_datetime_from_liq_dict(sell_array[-1], "Sell")).seconds == 60:
                    self.logger.info("Returning Sell signal based on big Sell liquidations spike")
                    return True
            if sa[-1] < -self.sell_spike_factor*th and sa[-2] > self.sell_spike_factor*th:
                if sell_array[-1] / sell_array[-2] < 0.3 and sell_array[-3] / sell_array[-2] < 0.3:
                    if sell_array[-1] > self.minimum_liquidations and sell_array[-3] > self.minimum_liquidations:
                        self.logger.info("Returning Sell signal based on 'Spiky hill' Sell liquidations pattern")
                        return True

            if len(sell_array) > 3:
                if sa[-1] < -th and sa[-2] > th and sa[-3] < -th:
                    self.logger.info("Returning Sell signal based on 'downhill' Sell liquidations pattern")
                    return True
                elif sa[-2] > 0 and sa[-1] > 0:
                    if (self.get_datetime() - self.return_datetime_from_liq_dict(sell_array[-1], "Sell")).seconds == 60:
                        self.logger.info("Returning Buy signal based on 'cliff' liquidations pattern")
                        return True

            #return sa[-2] > 0 > sa[-1] and sell_array[-2] > self.liquidations_sell_thresh_hold
            return False

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

    def maintain_trade(self, symbol, stop, targets, quantity, amend_stop_price=True):
        quantity = int(quantity)
        position = self.true_get_position(symbol)
        position_size = self.get_position_size(position)
        stop_price = self.get_position_price(position)
        if abs(position_size) != quantity:
            self.win = True
            if abs(position_size) == int(self.config['OTHER']['Amount'])/3 and amend_stop_price:
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
