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
    bear_sell_array = []
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
    reset_stop = False
    stop_lmb = False
    stop_lhb = False
    stop_lsb = False
    liqs_overall_power = 0
    liqs_overall_power_ratio = 0
    bullish_factor_threshold = {
        'downhill': 0.49,
        'bear': 0.45,
        'lsb': 0.25,
        'lmb': 0.25,
        'lhb': 0.25
    }
    liq_factor_bar = 2.94
    bullish_factor_last_updated = False
    trade_start_time = False

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
            'bear': False,
            'lsb': False,
            'lmb': False,
            'lhb': False
        }
        self.average_candle_count = int(self.config['OTHER']['AverageCandleCount'])
        self.entry['downhill'] = [
            self.config['LiquidationEntries']['DownHillMin'],
            self.config['LiquidationEntries']['DownHillMax']
            ]
        self.entry['lsb'] = [
            self.config['LiquidationEntries']['LsbMin'],
            self.config['LiquidationEntries']['LsbMax']
        ]
        self.entry['lmb'] = [
            self.config['LiquidationEntries']['LmbMin'],
            self.config['LiquidationEntries']['LmbMax']
        ]
        self.entry['lhb'] = [
            self.config['LiquidationEntries']['LhbMin'],
            self.config['LiquidationEntries']['LhbMax']
        ]
        self.entry['bear'] = [
            self.config['LiquidationEntries']['BearMin'],
            self.config['LiquidationEntries']['BearMax']
            ]
        self.liq_factor_bar = float(self.config['OTHER']['liqFactorBar'])
        self.liquidations_buy_thresh_hold = 201426
        self.liquidations_sell_thresh_hold = 101241
        self.liqs_factor = 0
        self.minimum_liquidations = 900
        self.last_downhill = self.get_start_date()
        self.last_spiky_hill = self.get_start_date()
        bot_logger = Logger()
        self.bullish_factor_last_updated = self.get_datetime()
        self.logger = bot_logger.init_logger()
        self.logger.info('Boti Trading system initiated')

    def __destruct(self):
        self.logger.info('---------------------------------- End !!!!! ----------------------------------')

    @staticmethod
    def get_vwap_price_diff(vwap, price):
        return (abs(vwap - price)/price) * 100

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

    def update_liqs_factor(self, liquidation_list, interval, mod):
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

        liquidations_buy_thresh_hold = 1
        liquidations_sell_thresh_hold = 1
        if len(buy_array) > 3:
            liquidations_buy_thresh_hold = buy_array[len(buy_array) - int(len(buy_array) * 0.8)]
        if len(sell_array) > 3:
            liquidations_sell_thresh_hold = sell_array[len(sell_array) - int(len(sell_array) * 0.8)]
        self.liqs_factor = liquidations_buy_thresh_hold / liquidations_sell_thresh_hold
        self.liqs_overall_power = sum(buy_array) + sum(sell_array)
        self.liqs_overall_power_ratio = sum(buy_array) / sum(sell_array)

    def update_buy_sell_counter_trend(self, liquidation_list, interval, mod):
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
        self.liqs_factor = self.liquidations_buy_thresh_hold / self.liquidations_sell_thresh_hold
        self.liqs_overall_power = sum(buy_array) + sum(sell_array)
        self.liqs_overall_power_ratio = sum(buy_array) / sum(sell_array)

    def update_liquidation_dict(self):
        _now = datetime.datetime.strptime(self.liq_current_time_no_seconds(), '%d/%m/%Y, %H:%M')
        from_time_in_minutes = (_now - datetime.timedelta(seconds=2400))
        self.liquidations_dict = self.get_current_liquidations_dict(from_time_in_minutes)

    def determine_targets_factor(self):
        return self.signal['signal']

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

    def check_liqs_small_bullish_thresh_hold(self, signal_args):
        vwap = signal_args['vwap']
        last_price = self.get_last_price_close(signal_args['symbol'])
        if not self.enable_trade['lsb'] or signal_args['side'] is "Sell":
            return False
        if self.stop_lsb:
            return False
        if self.liqs_factor > self.liq_factor_bar:
            print("{} Bullish liqs with small distance True: {}, bullish factor: {}, distance: {}, price: {}".format(
                self.get_date(),
                self.liqs_factor,
                self.bullish_factor,
                self.get_vwap_price_diff(vwap, last_price),
                last_price)
            )
            self.stop_lsb = True
            price = self.get_last_price_close(signal_args['symbol'])
            return {'signal': 'lsb', 'fill_time': 7200, 'price': price}

    def check_liqs_medium_bullish_thresh_hold(self, signal_args):
        vwap = signal_args['vwap']
        last_price = self.get_last_price_close(signal_args['symbol'])
        if not self.enable_trade['lmb'] or signal_args['side'] is "Sell":
            return False
        if self.stop_lmb:
            return False
        if self.liqs_factor > self.liq_factor_bar:
            print("{} Bullish liqs with medium distance True: {}, bullish factor: {}, distance: {}, price: {}".format(
                self.get_date(),
                self.liqs_factor,
                self.bullish_factor,
                self.get_vwap_price_diff(vwap, last_price),
                last_price)
            )
            self.stop_lmb = True
            price = self.get_last_price_close(signal_args['symbol'])
            return {'signal': 'lmb', 'fill_time': 7200, 'price': price}

    def check_liqs_high_bullish_thresh_hold(self, signal_args):
        vwap = signal_args['vwap']
        last_price = self.get_last_price_close(signal_args['symbol'])
        if not self.enable_trade['lhb'] or signal_args['side'] is "Sell":
            return False
        if self.stop_lhb:
            return False
        if self.liqs_factor > self.liq_factor_bar:
            print("{} Bullish liqs with high distance True: {}, bullish factor: {}, distance: {}, price: {}".format(
                self.get_date(),
                self.liqs_factor,
                self.bullish_factor,
                self.get_vwap_price_diff(vwap, last_price),
                last_price)
            )
            self.stop_lhb = True
            price = self.get_last_price_close(signal_args['symbol'])
            return {'signal': 'lhb', 'fill_time': 7200, 'price': price}

    def check_downhill(self, signal_args):
        symbol = signal_args['symbol']
        side = signal_args['side']
        buy_array = signal_args['buy_array']
        sell_array = signal_args['sell_array']
        diff_array = signal_args['diff_array']
        th = signal_args['th']
        vwap = signal_args['vwap']
        if not self.enable_trade['downhill'] or self.liqs_factor > self.liq_factor_bar:
            return False
        if buy_array[-1] < self.minimum_liquidations or side is "Sell" or sell_array[-1] > self.minimum_liquidations:
            return False
        try:
            if diff_array[-1] < -th and diff_array[-2] > th and diff_array[-3] < -self.minimum_liquidations:
                if 120 > (self.get_datetime() - self.return_datetime_from_liq_dict(buy_array[-1], side)).seconds >= 60:
                    last_price = self.get_last_price_close(symbol)
                    price = last_price - (vwap - last_price) * 0.618
                    print(
                        '{} Returning positive signal based on downhill pattern, bullish factor: {} diff:{}'.format(
                            self.get_date(), self.bullish_factor, self.get_vwap_price_diff(vwap, price))
                    )
                    self.logger.info(
                        "Returning positive 'downhill', bullish factor: {}, diff: {}".format(
                            self.bullish_factor, self.get_vwap_price_diff(vwap, price))
                    )
                    return {'signal': 'downhill', 'fill_time': 7200, 'price': price}
        except Exception as e:
            self.logger.warning('downhill exception: {}'.format(e))
        if '--Test' not in sys.argv:
            self.logger.info("Downhill returned False, side:{} liq dict:{}".format(side, self.liquidations_dict))
        return False

    def check_bear(self, signal_args):
        side = signal_args['side']
        symbol = signal_args['symbol']
        sell_array = signal_args['sell_array']
        th = signal_args['th']
        vwap = signal_args['vwap']
        if side is "Buy" or not self.enable_trade['bear']:
            if self.bear_sell_array:
                print("{} Resetting bear sell price array.".format(self.get_date()))
                self.logger.info("Resetting bear price array.")
                self.bear_sell_array = []
            return False

        kline = self.get_last_kline(symbol, '1')
        last_price = kline['close']
        if self.bear_sell_array and self.bear_sell_array[-1] < last_price + last_price * 0.002:
            if self.bear_sell_array[-1] < kline['high']:
                price = last_price
            else:
                price = self.bear_sell_array[-1]
            self.bear_sell_array.remove(self.bear_sell_array[-1])
            print('{} bear price True, price: {}, bull factor: {}'.format(self.get_date(), price, self.bullish_factor))
            self.logger.info("bear price True, price: {}, bull factor: {}".format(price, self.bullish_factor))
            return {'signal': 'bear', 'fill_time': 1440, 'price': price}

        curr_time = self.get_datetime().strftime('%d/%m/%Y, %H:%M')
        if curr_time in self.liquidations_dict:
            buy_liqs = self.liquidations_dict[curr_time]['Buy']
        else:
            return False
        if buy_liqs > self.minimum_liquidations:
            i = 1
            try:
                while sell_array[-i] != 0:
                    if len(sell_array) <= 2:
                        return False
                    i += 1
                if sell_array[-1-i] > th and sell_array[-i-1] > sell_array[i]:
                    price = kline['high']
                    p236 = (price - vwap)*0.236 + price
                    p618 = (price - vwap)*0.62 + price
                    price_diff = self.get_vwap_price_diff(vwap, kline['close'])

                    print('{} Positive Bear factor: {}, price:{}, 236: {} 618: {} diff:{}'.format(
                        self.get_date(), self.bullish_factor, price, p236, p618, price_diff))
                    self.logger.info(
                        'Positive Bear factor: {}, price:{}, 236: {} 618: {} diff:{}'.format(
                            self.bullish_factor, price, p236, p618, price_diff
                        )
                    )
                    if price_diff < 0.5:
                        price = p236
                    else:
                        price = p618
                    self.bear_sell_array.append(price)
                    self.bear_sell_array.sort()
            except Exception as e:
                print('{} exception: {}'.format(self.get_date(), e))
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
        if float(self.entry[signal][1]) > self.get_vwap_price_diff(vwap, last_price) > float(self.entry[signal][0]):
            return True
        else:
            return False

    def enable_bearish_signal(self, signal):
        if self.bullish_factor > self.bullish_factor_threshold[signal] and self.enable_trade[signal]:
            print("{} Stopping {} as bullish factor doesn't meet constrains: {}".format(
                self.get_date(), signal, self.bullish_factor))
            self.logger.info("Stopping {} as bullish factor doesn't meet constrains: {}".format(
                signal, self.bullish_factor))
            self.enable_trade[signal] = False
            return False
        elif self.bullish_factor < self.bullish_factor_threshold[signal] and not self.enable_trade[signal]:
            print(
                "{} Enabling {} as bullish factor sufficient: {}".format(self.get_date(), signal, self.bullish_factor)
            )
            self.logger.info("Enabling {} as bullish factor is sufficient: {}".format(signal, self.bullish_factor))
            self.enable_trade[signal] = True
            return True
        else:
            return self.enable_trade[signal]

    def enable_bullish_signal(self, signal):
        if self.bullish_factor < self.bullish_factor_threshold[signal] and self.enable_trade[signal]:
            print("{} Stopping {} as bullish factor too low: {}".format(self.get_date(), signal, self.bullish_factor))
            self.logger.info("Stopping {} as bullish factor too low: {}".format(signal, self.bullish_factor))
            self.enable_trade[signal] = False
            return False
        elif self.bullish_factor > self.bullish_factor_threshold[signal] and not self.enable_trade[signal]:
            print(
                "{} Enabling {} as bullish factor sufficient: {}".format(self.get_date(), signal, self.bullish_factor)
            )
            self.logger.info("Enabling {} as bullish factor is sufficient: {}".format(signal, self.bullish_factor))
            self.enable_trade[signal] = True
            return True
        else:
            return self.enable_trade[signal]

    def enable_signal(self, signal):
        if signal == 'bear':
            return self.enable_bearish_signal(signal)
        else:
            return self.enable_bullish_signal(signal)

    def get_liquidations_signal(self, symbol, side, vwap, last_price):
        signals = ['lsb', 'lmb', 'lhb', 'downhill', 'bear']
        check_signal = []
        sell_array = []
        buy_array = []
        diff_array = []
        th = 0
        if vwap < last_price:
            self.stop_lmb = False
            self.stop_lhb = False
            self.stop_lsb = False
        for sig in signals:
            if self.enable_signal(sig) and self.check_entry(last_price, vwap, sig):
                check_signal.append(sig)
        if not check_signal:
            return False

        for k in self.liquidations_dict.keys():
            sell_array.append(self.liquidations_dict[k]["Sell"])
            buy_array.append(self.liquidations_dict[k]["Buy"])
        buy_array.reverse()
        sell_array.reverse()
        if side is "Buy" and len(buy_array) < 3 or side is "Sell" and len(sell_array) < 3:
            return False
        self.update_buy_sell_thresh_hold(self.return_liquidations(), 4, 1)
        self.update_liqs_factor(self.return_liquidations(), 4, 15)

        if side is "Buy":
            diff_array = np.diff(buy_array)
            th = self.liquidations_buy_thresh_hold

        if side is "Sell":
            diff_array = np.diff(sell_array)
            th = self.liquidations_sell_thresh_hold

        if self.live:
            self.logger.info('bullish factor: {}, liqs factor: {}, over all factor: {} distance: {}'.format(
                self.bullish_factor, self.liqs_factor, self.liqs_overall_power_ratio,
                self.get_vwap_price_diff(vwap, last_price)
            ))
        for check_sig in check_signal:
            if check_sig == 'downhill':
                _check = self.check_downhill
            elif check_sig == 'bear':
                _check = self.check_bear
            elif check_sig == 'lsb':
                self.liq_factor_bar = float(self.config['OTHER']['liqFactorBar'])
                _check = self.check_liqs_small_bullish_thresh_hold
            elif check_sig == 'lmb':
                self.liq_factor_bar = float(self.config['OTHER']['liqFactorBar'])
                _check = self.check_liqs_medium_bullish_thresh_hold
            elif check_sig == 'lhb':
                self.liq_factor_bar = float(self.config['OTHER']['liqFactorBar'])
                _check = self.check_liqs_high_bullish_thresh_hold
            else:
                _check = self.check_downhill
            signal_args = {
                'symbol': symbol,
                'side': side,
                'buy_array': buy_array,
                'sell_array': sell_array,
                'diff_array': diff_array,
                'th': th,
                'vwap': vwap
            }
            sig = _check(signal_args)
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
        self.trade_start_time = self.get_datetime()
        self.orders.append(self.create_stop(symbol, stop_px))
        position = self.true_get_position(symbol)
        self.logger.info("Current Trade, symbol: {} side: {} size: {} price: {}".format(
            symbol,
            self.get_position_side(position),
            self.get_position_size(position),
            self.get_position_price(position)
        ))
        position_price = self.get_position_price(position)
        quantity = int(quantity)
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
        reset_interval = False
        if self.trade_start_time and abs(position_size) == int(self.initial_amount):
            reset_interval = self.get_datetime() - self.trade_start_time
        if reset_interval and reset_interval.seconds > int(self.stop_reset_time[self.signal['signal']]) * 60 * 60:
            self.trade_start_time = False
            self.reset_stop = True
            self.logger.info('Amending Stop as it reached time constrains.')
            print("{} Amending Stop as it reached time constrains.".format(self.get_date()))
            stop_price = str(int(stop_price))
            self.edit_stop(symbol, stop, quantity, stop_price)

        if abs(position_size) != quantity:
            self.win = True
            if abs(position_size) == int(self.initial_amount) / 3 and amend_stop_price:
                position_side = self.get_position_side(position)
                if position_side == 'Sell':
                    stop_price = stop_price - targets[0] * stop_price
                else:
                    stop_price = stop_price + targets[0] * stop_price
            elif self.reset_stop:
                quantity = abs(position_size)
                return quantity
            stop_price = str(int(stop_price))
            quantity = abs(position_size)
            self.logger.info("Amending stop as limit was filled, price:{} quantity:{}".format(stop_price, quantity))
            self.edit_stop(symbol, stop, quantity, stop_price)
        return quantity
