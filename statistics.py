from bybit_tools import BybitTools


class Statistics(BybitTools):
    bullish_factor_array = []
    bullish_factor = 0

    def __init__(self):
        super(Statistics, self).__init__()
        self.logger.info('Applying Liquidations Strategy')

    def next(self):
        symbol = "BTCUSD"
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
            self.update_buy_sell_thresh_hold(self.liqs, 4, 15)
            print("{} Buy: {}, Sell: {}, Ratio: {}".format(
                self.get_date(),
                self.liquidations_buy_thresh_hold,
                self.liquidations_sell_thresh_hold,
                self.liquidations_buy_thresh_hold / self.liquidations_sell_thresh_hold
            ))
            print("{} Bullish factor: {}".format(self.get_date(), self.bullish_factor))
