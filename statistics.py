from bybit_tools import BybitTools


class Statistics(BybitTools):
    bullish_factor_array = []
    bullish_factor = 0

    def __init__(self):
        super(Statistics, self).__init__()
        self.logger.info('Applying Liquidations Strategy')

    def next(self):
        symbol = "BTCUSD"
        days_interval = 4
        if days_interval == 4:
            power100 = 3000000
        elif days_interval == 2:
            power100 = 4699978
        else:
            power100 = 5892262
        if days_interval == 4:
            overall_divider = 4869171
        elif days_interval == 2:
            overall_divider = 3477793
        else:
            overall_divider = 2548794
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        if last_price > vwap:
            self.bullish_factor_array.insert(0, 1)
        else:
            self.bullish_factor_array.insert(0, 0)
        if len(self.bullish_factor_array) > 1500:
            self.bullish_factor_array.pop()
        self.bullish_factor = sum(self.bullish_factor_array) / len(self.bullish_factor_array)

        dt = self.get_datetime()
        if dt.minute == 0 or dt.minute == 30:
            self.update_buy_sell_thresh_hold(self.return_liquidations(), days_interval, 15)
            self.update_liqs_factor(self.return_liquidations(), days_interval, 15)
            print("{} Buy: {}, Sell: {}, Ratio: {}, Power Factor: {}, Overall Factor: {}, Overall Ratio: {}".format(
                self.get_date(),
                self.liquidations_buy_thresh_hold,
                self.liquidations_sell_thresh_hold,
                self.liqs_factor,
                round(((self.liquidations_buy_thresh_hold + self.liquidations_buy_thresh_hold) / power100) * 100, 2),
                round(self.liqs_overall_power / overall_divider, 2),
                self.liqs_overall_power_ratio
            ))
            print("{} Bullish factor: {}".format(self.get_date(), self.bullish_factor))
