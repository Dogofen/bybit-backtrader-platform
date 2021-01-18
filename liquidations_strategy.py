import datetime
from time import sleep
from bybit_tools import BybitTools


class LiquidationStrategy(BybitTools):
    targets = {'Long': [], 'Short': []}
    stop_px = {'Long': '0', 'Short': '0'}
    amount = False
    limit_order_time = False
    in_a_trade = False
    win = False
    _wait = False
    price_above = False
    last_vwap = False
    long_entries = []
    short_entries = []
    entry_counter = 0

    def __init__(self):
        super(LiquidationStrategy, self).__init__()
        self.targets["Long"] = [
                float(self.config["LiquidationLongTarget"]["Target0"]),
                float(self.config["LiquidationLongTarget"]["Target1"]),
                float(self.config["LiquidationLongTarget"]["Target2"])
             ]
        self.targets["Short"] = [
                float(self.config["LiquidationShortTarget"]["Target0"]),
                float(self.config["LiquidationShortTarget"]["Target1"]),
                float(self.config["LiquidationShortTarget"]["Target2"])
            ]
        self.long_entries = [
            self.config['LiquidationLongEntries']['LongEntry0'],
            self.config['LiquidationLongEntries']['LongEntry1'],
            self.config['LiquidationLongEntries']['LongEntry2'],
            self.config['LiquidationLongEntries']['LongEntry3']
            ]
        self.short_entries = [
            self.config['LiquidationShortEntries']['ShortEntry0'],
            self.config['LiquidationShortEntries']['ShortEntry1'],
            self.config['LiquidationShortEntries']['ShortEntry2'],
            self.config['LiquidationShortEntries']['ShortEntry3']
            ]
        self.stop_px["Long"] = self.config["LiquidationStops"]["LongStopPx"]
        self.stop_px["Short"] = self.config["LiquidationStops"]["ShortStopPx"]
        self.amount = self.config["OTHER"]["Amount"]
        self.logger.info('Applying Liquidations Strategy')

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
        self.amount = self.maintain_trade(symbol, stop, self.targets[self.is_long_position(symbol)], self.amount)

    def start_trade(self, symbol, position):
        print("Trade started time:{}".format(self.get_date()))
        long_or_short = self.is_long_position(symbol)
        self.in_a_trade = True
        if len(self.orders) == 1:
            self.limit_order_time = False
            self.orders.pop()
            side = self.get_position_side(position)
            self.initiate_trade(
                symbol,
                self.amount,
                side,
                self.targets[long_or_short],
                self.stop_px[long_or_short] + '%'
            )

    def adjust_order_to_vwap(self, symbol, vwap):
        if self.last_vwap != vwap and vwap:
            self.edit_orders_price(symbol, self.orders[0], vwap)
        return vwap

    def put_limit_order(self, symbol, vwap, last_price):
        if last_price > vwap:
            if ((last_price / vwap) - 1) * 100 > float(self.short_entries[self.entry_counter]):
                sig = self.get_liquidations_signal(symbol, "Sell")
                if sig:

                    self.logger.info(
                        "Creating Limit order with side: Sell and entry: {}".format(
                            self.short_entries[self.entry_counter]
                        )
                    )
                    self.logger.info(
                        "liquidations thresh hold for Sell: {} liquidations are {}".format(
                            self.liquidations_sell_thresh_hold, self.liquidations_dict
                        )
                    )
                    self.fill_time = sig['fill_time']
                    self.orders.append(self.limit_order(symbol, "Sell", self.amount, sig['price']))
                    self.limit_order_time = self.get_datetime()
        if last_price < vwap:
            if ((vwap / last_price) - 1) * 100 > float(self.long_entries[self.entry_counter]):
                sig = self.get_liquidations_signal(symbol, "Buy")
                if sig:
                    self.logger.info(
                        "Creating Limit order with side: Buy and entry: {}".format(
                            self.long_entries[self.entry_counter]
                        )
                    )
                    self.logger.info(
                        "liquidations thresh hold for Buy: {} liquidations are {}".format(
                            self.liquidations_buy_thresh_hold, self.liquidations_dict
                        )
                    )
                    self.fill_time = sig['fill_time']
                    self.orders.append(self.limit_order(symbol, "Buy", self.amount, sig['price']))
                    self.limit_order_time = self.get_datetime()

    def zero_entry_counter(self, last_price, vwap):
        if last_price < vwap and self.price_above and self.entry_counter != 0:  # Price just crossed vwap
            self.entry_counter = 0
            self.logger.info("Zeroing entry counter as price crossed vwap")
        if last_price > vwap and not self.price_above and self.entry_counter != 0:  # Price just crossed vwap
            self.entry_counter = 0
            self.logger.info("Zeroing entry counter as price crossed vwap")

    def price_above_below_vwap(self, last_price, vwap):
        if last_price > vwap:  # This determines if price has crossed vwap
            self.price_above = True
        else:
            self.price_above = False

    def strategy_run(self, symbol, position, last_price, vwap):
        position_size = self.get_position_size(position)
        self.update_liquidation_dict(symbol)
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

        if self.limit_order_time and (self.get_datetime() - self.limit_order_time).seconds > self.fill_time:
            self.logger.info("Cancelling order as it didn't met time constrains")
            self.limit_order_time = False
            self.cancel_order(symbol, self.orders[0])

    def next(self):
        symbol = "BTCUSD"
        vwap = self.get_vwap(symbol)
        last_price = self.get_last_price_close(symbol)
        position = self.true_get_position(symbol)
        self.strategy_run(symbol, position, last_price, vwap)
        while self.live:
            if datetime.datetime.now().second and not self.in_a_trade > 10:
                sleep(1)
                continue
            if self.in_a_trade:
                sleep(5)
            vwap = self.get_vwap(symbol)
            last_price = self.get_last_price_close(symbol)
            position = self.true_get_position(symbol)
            self.strategy_run(symbol, position, last_price, vwap)
