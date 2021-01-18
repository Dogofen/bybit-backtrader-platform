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
    fill_time = 0
    average_candle_count = 0
    sell_spike_factor = 0
    buy_spike_factor = 0
    minimum_liquidations = 0
    liquidations_dict = {}
    liquidations_buy_thresh_hold = 0
    liquidations_sell_thresh_hold = 0

    def __init__(self):
        super(BybitTools, self).__init__()
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

        self.sell_spike_factor = 3
        self.buy_spike_factor = 5
        self.fill_time = 120
        self.average_candle_count = int(self.config['OTHER']['AverageCandleCount'])
        self.liquidations_buy_thresh_hold = 261426
        self.liquidations_sell_thresh_hold = 101241
        self.minimum_liquidations = 900
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.logger.info('Boti Trading system initiated')

    def __destruct(self):
        self.logger.info('---------------------------------- End !!!!! ----------------------------------')

    def get_day_open(self):
        date_now = self.get_datetime()
        date_from = datetime.datetime.strptime(date_now.strftime('%Y-%m-%d ' '%H:00:00'), '%Y-%m-%d ' '%H:%M:%S')
        day_open = self.get_time_open()
        while date_from.strftime('%H:%M:%S') != day_open:
            date_from = date_from - datetime.timedelta(hours=1)
        return date_from.timestamp()

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

    def update_buy_sell_thresh_hold(self, liquidation_list, mod):
        liq_m_dict = {}
        sell_array = []
        buy_array = []
        for x in liquidation_list:
            dt = datetime.datetime.fromtimestamp(int(x['time'] / 1000))
            time = (dt - datetime.timedelta(minutes=dt.minute % mod)).strftime("%d/%m/%Y, %H:%M")
            if time not in liq_m_dict.keys():
                liq_m_dict[time] = {"Buy": 0, "Sell": 0}
            liq_m_dict[time][x['side']] += x['qty']
        for k in liq_m_dict.keys():
            sell_array.append(liq_m_dict[k]['Sell'])
            buy_array.append(liq_m_dict[k]['Buy'])

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

    def check_spike(self, array, side):
        if array[-1] > self.buy_spike_factor * self.liquidations_buy_thresh_hold:
            if (self.get_datetime() - self.return_datetime_from_liq_dict(array[-1], side)).seconds == 60:
                #print('Returning positive signal based on liquidations spike {}'.format(self.get_date()))
                self.logger.info("Returning positive signal based on big Buy liquidations spike")
                return False

    def check_spiky_hill(self, diff_array, array, th):
        if diff_array[-1] < -self.sell_spike_factor * th and diff_array[-2] > self.sell_spike_factor * th:
            if array[-1] / array[-2] < 0.3 and array[-3] / array[-2] < 0.3:
                if array[-1] > self.minimum_liquidations and array[-3] > self.minimum_liquidations:
                    #print('Returning positive signal based on spiky_hill pattern {}'.format(self.get_date()))
                    self.logger.info("Returning positive signal based on 'Spiky hill' liquidations pattern")
                    return False

    def check_downhill(self, diff_array, th):
        if diff_array[-1] < -th and diff_array[-2] > th and diff_array[-3] < -th:
            #print('Returning positive signal based on downhill pattern {}'.format(self.get_date()))
            self.logger.info("Returning positive signal based on 'downhill' Buy liquidations pattern")
            return False

    def check_cliff(self, symbol, diff_array, array, side, average_candle):
        lc = self.get_last_kline(symbol, '1')
        candle_length = lc['high'] - lc['low']
        if candle_length < average_candle:
            return False
        if diff_array[-2] > 0 and diff_array[-1] > 0:
            if side is "Buy":
                price_array = []
                last_bars = self.get_kline(symbol, '1', self.get_time_delta(90))
                for b in last_bars:
                    price_array.append(b['low'])
                minimum = min(price_array)
                if lc['close'] < minimum or (lc['close'] - minimum) / lc['close'] < 0.015:
                    return False
            if (self.get_datetime() - self.return_datetime_from_liq_dict(array[-1], side)).seconds == 60:
                print(
                    'Returning positive {} signal based on cliff pattern: {} last_candle length: {} average:{}'.format(
                        side,
                        self.get_date(),
                        candle_length,
                        average_candle
                    )
                )
                self.logger.info("Returning positive signal based on 'cliff' liquidations pattern")
                if side is "Buy":
                    if lc['open'] < lc['close']:
                        when = 'open'
                    else:
                        when = 'close'
                    price = (lc["low"] + 2 * lc[when]) / 3
                else:
                    if lc['open'] > lc['close']:
                        when = 'open'
                    else:
                        when = 'close'
                    price = (lc["high"] + 2 * lc[when]) / 3
                return {'signal': 'cliff', 'fill_time': 180, 'price': price}

    def get_average_candle(self, symbol, count):
        klines_array = []
        _from = self.get_time_delta(count)
        klines = self.get_kline(symbol, '1', _from)
        for k in klines:
            klines_array.append(k['high'] - k['low'])
        return sum(klines_array) / len(klines_array)

    def get_liquidations_signal(self, symbol, side):
        sell_array = []
        buy_array = []
        array = []
        diff_array = []
        th = 0
        average_candle = self.get_average_candle(symbol, self.average_candle_count)
        for k in self.liquidations_dict.keys():
            sell_array.append(self.liquidations_dict[k]["Sell"])
            buy_array.append(self.liquidations_dict[k]["Buy"])
        if side is "Buy":
            if len(buy_array) < 3:
                return False
            buy_array.reverse()
            array = buy_array
            diff_array = np.diff(buy_array)
            th = self.liquidations_buy_thresh_hold

        if side is "Sell":
            if len(sell_array) < 3:
                return False
            sell_array.reverse()
            array = sell_array
            diff_array = np.diff(sell_array)
            th = self.liquidations_sell_thresh_hold

        if self.check_spike(array, side):
            return True

        if self.check_spiky_hill(diff_array, array, th):
            return True

        if len(array) > 3:
            if self.check_downhill(diff_array, th):
                return True
            sig = self.check_cliff(symbol, diff_array, array, side, average_candle)
            if sig:
                return sig

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
