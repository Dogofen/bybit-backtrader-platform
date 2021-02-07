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
    signal = {}
    enable_trade = {}
    live = False
    fill_time = 0
    average_candle_count = 0
    spike_factor = 0
    vlf_bullish_price = []
    last_spiky_hill = False
    last_downhill = False
    minimum_liquidations = 0
    liquidations_buy_thresh_hold = 0
    liquidations_sell_thresh_hold = 0
    bullish_factor_array = []
    bullish_factor = 0
    entry = {}
    bullish_factor_threshold = 0.49
    bullish_factor_last_updated = False

    def __init__(self):
        super(BybitTools, self).__init__()
        if "--Test" in sys.argv:
            self.live = False
        else:
            self.live = True

        self.spike_factor = 4
        self.fill_time = 120
        self.enable_trade = {
            'downhill': False,
            'cliff': False
        }
        self.average_candle_count = int(self.config['OTHER']['AverageCandleCount'])
        self.entry['downhill'] = self.config['LiquidationEntries']['DownHill']
        self.liquidations_buy_thresh_hold = 201426
        self.liquidations_sell_thresh_hold = 101241
        self.minimum_liquidations = 900
        self.last_downhill = self.get_start_date()
        self.last_spiky_hill = self.get_start_date()
        bot_logger = Logger()
        self.bullish_factor_last_updated = self.get_datetime()
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

    def update_bullish_factor(self, vwap, last_price):
        dt = self.get_datetime()
        delta = dt - self.bullish_factor_last_updated
        if delta.seconds < 30:
            return
        elif delta.seconds > 90:
            self.logger.warning("Bullish factor may not be updated correctly.")
            self.bullish_factor_last_updated = dt
        else:
            self.bullish_factor_last_updated = dt

        if last_price > vwap:
            self.bullish_factor_array.insert(0, 1)
        else:
            self.bullish_factor_array.insert(0, 0)
        if len(self.bullish_factor_array) > 1500:
            self.bullish_factor_array.pop()
        self.bullish_factor = sum(self.bullish_factor_array) / len(self.bullish_factor_array)

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

    def update_buy_sell_thresh_hold(self, liquidation_list, interval, mod):
        liq_m_dict = {}
        sell_array = []
        buy_array = []
        now = self.get_datetime()
        start = now - datetime.timedelta(days=interval)
        for x in liquidation_list:
            dt = datetime.datetime.fromtimestamp(int(x['time'] / 1000))
            if dt > now or dt < start:
                continue
            dt = dt - datetime.timedelta(minutes=dt.minute % mod)
            time = dt.strftime("%d/%m/%Y, %H:%M")
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

    def update_liquidation_dict(self):
        _now = datetime.datetime.strptime(self.liq_current_time_no_seconds(), '%d/%m/%Y, %H:%M')
        from_time_in_minutes = (_now - datetime.timedelta(seconds=2400))
        self.liquidations_dict = self.get_current_liquidations_dict(from_time_in_minutes)

    def determine_targets_factor(self, symbol):
        if self.signal['signal'] is 'downhill':
            self.logger.info("Targets factor is downhill")
            return 'downhill'
        daily_range = self.get_daily_range(symbol)
        last_price = self.get_last_price_close(symbol)
        if daily_range/last_price < 0.05:
            self.logger.info("Targets factor is low {}".format(daily_range/last_price))
            return 'low'
        if 0.09 > daily_range/last_price > 0.05:
            self.logger.info("Targets factor is medium {}".format(daily_range/last_price))
            return 'medium'
        else:
            self.logger.info("Targets factor is high {}".format(daily_range/last_price))
            return 'high'

    def check_bullish_vwap_liquidation_fibonacci(self, symbol, array, side, vwap):
        if side is "Buy":
            last_kline = self.get_last_kline(symbol, '1')
            last_price = last_kline['close']
            if self.vlf_bullish_price and self.vlf_bullish_price[-1] > last_price - last_price * 0.002:
                if self.vlf_bullish_price[-1] > last_kline['low']:
                    price = last_price
                else:
                    price = self.vlf_bullish_price[-1]
                self.vlf_bullish_price.remove(self.vlf_bullish_price[-1])
                print('{} vwap_liquidation_fibonacci True'.format(self.get_date()))
                self.logger.info("vwap_liquidation_fibonacci True, price: {}".format(price))
                return {'signal': 'vlf', 'fill_time': 1440, 'price': price}
            if array[-1] > self.spike_factor * self.liquidations_buy_thresh_hold:
                if 120 > (self.get_datetime() - self.return_datetime_from_liq_dict(array[-1], side)).seconds >= 60:
                    for factor in [0.236, 0.618]:
                        price = last_price - (vwap - last_price) * factor
                        print('{} vwap_liquidation_fibonacci price update, liqs are: {} factor: {} price: {}'.format(
                            self.get_date(), array[-1], factor, price)
                        )
                        self.logger.info(
                            "vwap_liquidation_fibonacci price update, liqs are: {} factor: {} price: {}".format(
                                array[-1], factor, price)
                        )
                        self.vlf_bullish_price.append(price)
                    self.vlf_bullish_price.sort()
        elif side is "Sell" and self.vlf_bullish_price:
            print("{} Resetting vlf price array.".format(self.get_date()))
            self.logger.info("Resetting vlf price array.")
            self.vlf_bullish_price = []
        return False

    #def check_bullish_hammer(self, symbol, side, buy_array, sell_array, diff_array):
    #    if side is "Sell":
    #        return False
    #    last_candle = self.get_last_kline(symbol, '1')
    #    if last_candle['close'] < last_candle['open']:
    #        return False
    #    if last_candle['open'] - last_candle['low'] > last_candle['close'] - last_candle['open']:
    #        print('We have a hammer at {}'.format(self.get_date()))
    #    return False

    def check_spiky_hill(self, symbol, side, diff_array, array, th):
        price_array = []
        if diff_array[-1] < -self.sell_spike_factor * th and diff_array[-2] > self.sell_spike_factor * th:
            if array[-1] / array[-2] < 0.3 and array[-3] / array[-2] < 0.3:
                if array[-1] > self.minimum_liquidations and array[-3] > self.minimum_liquidations:
                    first_delta = self.get_datetime() - self.last_downhill
                    second_delta = self.get_datetime() - self.last_spiky_hill
                    self.last_spiky_hill = self.get_datetime()
                    if first_delta.days > 0 or first_delta.seconds > 7200:
                        if second_delta.days > 0 or second_delta.seconds > 7200:
                            print(
                                '{} Returning positive signal based on spiky_hill pattern, factor: {}'.format(
                                    self.get_date(), self.bullish_factor)
                            )
                            self.logger.info("Returning positive signal based on 'Spiky hill' liquidations pattern")
                            last_bars = self.get_kline(symbol, '1', self.get_time_delta(6))
                            for lb in last_bars:
                                price_array.append(lb['high'])
                                price_array.append(lb['low'])
                            if side is "Buy":
                                price = min(price_array)
                            else:
                                price = max(price_array)
                            return {'signal': 'spiky_hill',  'fill_time': 960, 'price': price}
        return False

    def check_downhill(self, symbol, side, buy_array, sell_array, diff_array, th):
        if not self.enable_trade['downhill']:
            return False
        if buy_array[-1] < self.minimum_liquidations or side is "Sell" or sell_array[-1] > self.minimum_liquidations:
            if '--Test' not in sys.argv:
                self.logger.info("Downhill returned False, side:{} liq dict:{}".format(side, self.liquidations_dict))
            return False
        if diff_array[-1] < -th and diff_array[-2] > th and diff_array[-3] < -self.minimum_liquidations:
            if 120 > (self.get_datetime() - self.return_datetime_from_liq_dict(buy_array[-1], side)).seconds >= 60:
                print(
                    '{} Returning positive signal based on downhill pattern, bullish factor: {}'.format(
                        self.get_date(), self.bullish_factor)
                )
                self.logger.info(
                    "Returning positive signal based on 'downhill' Buy liquidations pattern, bullish factor: {}".format(
                        self.bullish_factor)
                )
                price = self.get_last_price_close(symbol)
                return {'signal': 'downhill', 'fill_time': 240, 'price': price}
        if '--Test' not in sys.argv:
            self.logger.info("Downhill returned False, side:{} liq dict:{}".format(side, self.liquidations_dict))
        return False

    def check_cliff(self, symbol, diff_array, array, side, average_candle):
        if self.stop_trade['cliff']:
            return
        lc = self.get_last_kline(symbol, '1')
        if not lc:
            self.logger.warning("Cliff could not finished, exiting with 'False'")
            return False
        candle_length = lc['high'] - lc['low']
        if candle_length < average_candle:
            if '--Test' not in sys.argv:
                self.logger.info("Cliff returned False, average candle: {} > candle length {}".format(
                    average_candle,
                    candle_length)
                )
            return False
        if diff_array[-2] > 0 and diff_array[-1] > 0:
            if side is "Buy":
                price_array = []
                last_bars = self.get_kline(symbol, '1', self.get_time_delta(90))
                for b in last_bars:
                    price_array.append(b['low'])
                minimum = min(price_array)
                if lc['close'] < minimum or (lc['close'] - minimum) / lc['close'] < 0.015:
                    if '--Test' not in sys.argv:
                        self.logger.info(
                            "Cliff returned False, side:{} liq dict:{}".format(side, self.liquidations_dict))
                    return False
            if 120 > (self.get_datetime() - self.return_datetime_from_liq_dict(array[-1], side)).seconds >= 60:
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
                    price = int((lc["low"] + 2 * lc[when]) / 3)
                else:
                    if lc['open'] > lc['close']:
                        when = 'open'
                    else:
                        when = 'close'
                    price = int((lc["high"] + 2 * lc[when]) / 3)
                return {'signal': 'cliff', 'fill_time': 540, 'price': price}
        if '--Test' not in sys.argv:
            self.logger.info("Cliff returned False, side:{} liq dict:{}".format(side, self.liquidations_dict))
        return False

    def get_average_candle(self, symbol, count):
        klines_array = []
        _from = self.get_time_delta(count)
        klines = self.get_kline(symbol, '1', _from)
        for k in klines:
            klines_array.append(float(k['high']) - float(k['low']))
        return sum(klines_array) / len(klines_array)

    def check_entry(self, last_price, vwap, signal):
        if ((last_price / vwap) - 1) * 100 > float(self.entry[signal]):
            return True
        elif ((vwap / last_price) - 1) * 100 > float(self.entry[signal]):
            return True
        else:
            return False

    def get_liquidations_signal(self, symbol, side, vwap, last_price):
        sell_array = []
        buy_array = []
        array = []
        diff_array = []
        th = 0
        if side == 'Sell':
            return False
        if not self.check_entry(last_price, vwap, 'downhill'):
            return False
        if self.bullish_factor < self.bullish_factor_threshold and self.enable_trade['downhill']:
            print("{} Stopping downhill as bullish factor too low: {}".format(self.get_date(), self.bullish_factor))
            self.logger.info("Stopping downhill as bullish factor too low: {}".format(self.bullish_factor))
            self.enable_trade['downhill'] = False
            return False
        if self.bullish_factor > self.bullish_factor_threshold and not self.enable_trade['downhill']:
            print("{} Enabling downhill as bullish factor sufficient: {}".format(self.get_date(), self.bullish_factor))
            self.logger.info("Enabling downhill as bullish factor is sufficient: {}".format(self.bullish_factor))
            self.enable_trade['downhill'] = True
        #average_candle = self.get_average_candle(symbol, self.average_candle_count)
        self.update_buy_sell_thresh_hold(self.return_liquidations(), 4, 1)

        for k in self.liquidations_dict.keys():
            sell_array.append(self.liquidations_dict[k]["Sell"])
            buy_array.append(self.liquidations_dict[k]["Buy"])
        buy_array.reverse()
        sell_array.reverse()
        if side is "Buy":
            if len(buy_array) < 3:
                return False
            array = buy_array
            diff_array = np.diff(buy_array)
            th = self.liquidations_buy_thresh_hold

        if side is "Sell":
            if len(sell_array) < 3:
                return False
            array = sell_array
            diff_array = np.diff(sell_array)
            th = self.liquidations_sell_thresh_hold

        #sig = self.check_bullish_hammer(symbol, side, buy_array, sell_array, diff_array)
        #if sig:
        #    return sig

        #sig = self.check_bullish_vwap_liquidation_fibonacci(symbol, array, side, vwap)
        #if sig:
        #    return sig

        if len(array) > 3:
            sig = self.check_downhill(symbol, side, buy_array, sell_array, diff_array, th)
            if sig:
                return sig
        #        sig = self.check_cliff(symbol, diff_array, array, side, average_candle)
        #        if sig:
        #            return sig

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
