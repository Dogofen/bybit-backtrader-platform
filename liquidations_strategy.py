import datetime
import pickle
from time import sleep
from bybit_tools import BybitTools
import os


class LiquidationStrategy(BybitTools):
    targets = {'Long': [], 'Short': []}
    stop_px = {'Long': '0', 'Short': '0'}
    amount = False
    initial_amount = False
    amount_percentage = False
    target_factor = 'low'
    coin = "BTC"
    limit_order_time = False
    in_a_trade = False
    win = False
    _wait = False
    price_above = False
    last_vwap = False

    def __init__(self):
        super(LiquidationStrategy, self).__init__()
        self.update_buy_sell_thresh_hold(self.return_liquidations(), 4, 1)
        self.update_liqs_factor(self.return_liquidations(), 4, 15)
        self.targets["bear"] = [
            float(self.config["BearTargets"]["Target0"]),
            float(self.config["BearTargets"]["Target1"]),
            float(self.config["BearTargets"]["Target2"])
        ]
        self.targets["downhill"] = [
            float(self.config["DownHillTargets"]["Target0"]),
            float(self.config["DownHillTargets"]["Target1"]),
            float(self.config["DownHillTargets"]["Target2"])
        ]
        self.targets["lsb"] = [
            float(self.config["LsbTargets"]["Target0"]),
            float(self.config["LsbTargets"]["Target1"]),
            float(self.config["LsbTargets"]["Target2"])
        ]
        self.targets["lmb"] = [
            float(self.config["LmbTargets"]["Target0"]),
            float(self.config["LmbTargets"]["Target1"]),
            float(self.config["LmbTargets"]["Target2"])
        ]
        self.targets["lhb"] = [
            float(self.config["LhbTargets"]["Target0"]),
            float(self.config["LhbTargets"]["Target1"]),
            float(self.config["LhbTargets"]["Target2"])
        ]
        self.stop_reset_time = {
            'downhill': self.config["DownHillTargets"]["StopReset"],
            'bear': self.config["BearTargets"]["StopReset"],
            'lsb': self.config["LsbTargets"]["StopReset"],
            'lmb': self.config["LmbTargets"]["StopReset"],
            'lhb': self.config["LhbTargets"]["StopReset"]
        }
        symbol = 'BTCUSD'
        self.stop_px["bear"] = self.config["BearTargets"]["StopPx"]
        self.stop_px["lmb"] = self.config["LmbTargets"]["StopPx"]
        self.stop_px["lhb"] = self.config["LhbTargets"]["StopPx"]
        self.stop_px["lsb"] = self.config["LsbTargets"]["StopPx"]
        self.stop_px["downhill"] = self.config["DownHillTargets"]["StopPx"]
        self.amount_percentage = float(self.config['OTHER']['AmountPercentage'])
        self.initial_amount = self.get_current_amount(symbol, self.amount_percentage)
        self.amount = self.initial_amount
        self.logger.info('Applying Liquidations Strategy starting with amount: {}'.format(self.initial_amount))

    def __del__(self):
        if self.live:
            with open('liquidations_Live', 'wb') as lq:
                pickle.dump(self.liquidations, lq)
            with open('bullish_factor_Live', 'wb') as lq:
                pickle.dump(self.bullish_factor_array, lq)

    def finish_operations_for_trade(self, symbol):
        print("Trade finished, win: {} time:{}  cash at the end: {}".format(
            self.win, self.get_date(), self.get_cash(self.coin)
        ))
        self.logger.info("Trade finished, win: {} time {}, cash at the end: {}".format(
            self.win, self.get_date(), self.get_cash(self.coin))
        )
        self.cancel_all_orders(symbol)
        self.in_a_trade = False
        self.reset_stop = False
        self.initial_amount = self.get_current_amount(symbol, self.amount_percentage)
        self.amount = self.initial_amount
        self.logger.info("New amount is : {}".format(self.initial_amount))
        self.win = False
        self.logger.info('---------------------------------- End ----------------------------------')

    def in_trade_operations(self, symbol):
        stop = self.get_stop_order()
        self.amount = self.maintain_trade(symbol, stop, self.targets[self.target_factor], self.amount)

    def start_trade(self, symbol, position):
        print("Trade started time:{}".format(self.get_date()))
        self.target_factor = self.determine_targets_factor()
        self.in_a_trade = True
        if len(self.orders) == 1:
            self.limit_order_time = False
            self.orders.pop()
            side = self.get_position_side(position)
            self.initiate_trade(
                symbol,
                self.amount,
                side,
                self.targets[self.target_factor],
                self.stop_px[self.target_factor] + '%'
            )

    def adjust_order_to_vwap(self, symbol, vwap):
        if self.last_vwap != vwap and vwap:
            self.edit_orders_price(symbol, self.orders[0], vwap)
        return vwap

    def get_daily_range(self, symbol):
        price_range = []
        day_open = self.get_day_open()
        kline = self.get_kline(symbol, self.interval, day_open)
        for k in kline:
            price_range.append(k['high'])
            price_range.append(k['low'])
        return max(price_range) - min(price_range)

    def put_limit_order(self, symbol, vwap, last_price):
        if last_price > vwap:
            self.signal = self.get_liquidations_signal(symbol, "Sell", vwap, last_price)
            if self.signal:
                self.logger.info("Creating Limit order with side: Sell.")
                self.logger.info(
                    "liquidations thresh hold for Sell: {} liquidations are {}".format(
                        self.liquidations_sell_thresh_hold, self.liquidations_dict
                    )
                )
                self.fill_time = self.signal['fill_time']
                self.orders.append(self.limit_order(symbol, "Sell", self.amount, self.signal['price']))
                self.limit_order_time = self.get_datetime()
        if last_price < vwap:
            self.signal = self.get_liquidations_signal(symbol, "Buy", vwap, last_price)
            if self.signal:
                self.logger.info("Creating Limit order with side: Buy.")
                self.logger.info(
                    "liquidations thresh hold for Buy: {} liquidations are {}".format(
                        self.liquidations_buy_thresh_hold, self.liquidations_dict
                    )
                )
                self.fill_time = self.signal['fill_time']
                self.orders.append(self.limit_order(symbol, "Buy", self.amount, self.signal['price']))
                self.limit_order_time = self.get_datetime()

    def strategy_run(self, symbol, position, last_price, vwap):
        if self.live:
            dt = self.get_datetime()
            if dt.second < 7:
                self.update_buy_sell_thresh_hold(self.return_liquidations(), 4, 1)
                self.update_liqs_factor(self.return_liquidations(), 4, 15)
                self.logger.info('bullish factor: {}, liqs factor: {}, over all factor: {} distance: {}'.format(
                    self.bullish_factor, self.liqs_factor, self.liqs_overall_power_ratio,
                    self.get_vwap_price_diff(vwap, last_price)
                ))
            self.sleep_if_rate_limit(position['rate_limit_status'])
        position_size = self.get_position_size(position)
        self.update_bullish_factor(vwap, last_price)
        dt = self.get_datetime()
        if not dt.minute % 30 and dt.second < 5 and not self.live:
            self.update_buy_sell_thresh_hold(self.return_liquidations(), 4, 1)
            self.update_liqs_factor(self.return_liquidations(), 4, 15)
            self.logger.info('{} bullish factor: {}, liqs factor: {}'.format(
                self.get_date(), self.bullish_factor, self.liqs_factor
            ))
        if position_size == 0 and self.in_a_trade:  # Finish Operations
            self.finish_operations_for_trade(symbol)

        if position_size != 0 and self.in_a_trade:  # When in Trade, maintaining
            self.in_trade_operations(symbol)

        if position_size != 0 and not self.in_a_trade:  # When limit order just accepted
            self.start_trade(symbol, position)

        if not self.in_a_trade and len(self.orders) == 0:
            self.update_liquidation_dict()
            self.put_limit_order(symbol, vwap, last_price)  # Send First Limit order

        if self.limit_order_time and (self.get_datetime() - self.limit_order_time).seconds > self.fill_time:
            self.logger.info("Cancelling order as it didn't met time constrains")
            self.limit_order_time = False
            self.cancel_order(symbol, self.orders[0])

        if self.live:
            self.update_liquidations(symbol)

    def next(self):
        symbol = "BTCUSD"
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        position = self.true_get_position(symbol)
        self.strategy_run(symbol, position, last_price, vwap)
        while self.live:
            if datetime.datetime.now().second > 10 and not self.in_a_trade:
                if datetime.datetime.now().second % 3:
                    self.update_liquidations(symbol)
                sleep(1)
                continue
            vwap = self.get_vwap(symbol)
            last_price = self.get_last_price_close(symbol)
            if not last_price:
                self.logger.warning("Last price could not be retrieved. repeating loop")
                continue
            position = self.true_get_position(symbol)
            self.strategy_run(symbol, position, last_price, vwap)
            if self.in_a_trade:
                sleep(5)
            if os.path.exists('close'):
                os.system('rm close')
                self.__del__()
                self.live = False
