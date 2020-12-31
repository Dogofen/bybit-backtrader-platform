import backtrader as bt
import configparser
from botlogger import Logger
import pickle


class BybitOperations(bt.Strategy):
    logger = ''
    orders = []
    order = ''
    day_open_dict = {
        '01': "02:00:00",
        '02': "02:00:00",
        '03': "02:00:00",
        '04': "03:00:00",
        '05': "03:00:00",
        '06': "03:00:00",
        '07': "03:00:00",
        '08': "03:00:00",
        '09': "03:00:00",
        '10': "03:00:00",
        '11': "02:00:00",
        '12': "02:00:00"}

    def __init__(self):
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        self.interval = self.config["Vwap"]["Interval"]

        self.logger.info("Finished BybitTools construction, proceeding")

    def create_order(self, order_type, symbol, side, amount, price):
        self.logger.info(
            "Sending a Create Order command time => {} type => {} side =>{} amount=>{} price=>{}".format(
                bt.num2date(self.datas[0].datetime[0]).strftime('%Y-%m-%d %H:%M:%S'), order_type, side, amount, price)
        )
        if order_type == 'Limit':
            order_type = bt.Order.Limit
        else:
            order_type = False

        if side == "Buy":
            order = self.buy(exectype=order_type, size=amount, price=price)
        else:
            order = self.sell(exectype=order_type, size=amount, price=price)
        return order

    def true_get_position(self, symbol):
        position = self.position
        return position

    def get_cash(self, coin):
        return self.broker.getcash()

    def get_time(self):
        return bt.num2date(self.datas[0].datetime[0]).strftime('%H:%M:%S')

    def get_day_open(self):
        return self.day_open_dict[self.get_month()]

    def true_get_active_orders(self, symbol):
        return self.orders

    def true_get_stop_order(self, symbol):
        return self.orders

    def get_month(self):
        return bt.num2date(self.datas[0].datetime[0]).strftime('%m')

    def get_date(self):
        return bt.num2date(self.datas[0].datetime[0]).strftime('%Y-%m-%d %H:%M:%S')

    def get_position_size(self, position):
        return position.size

    def get_position_side(self, position):
        if position.size > 0:
            return "Buy"
        return "Sell"

    def get_position_price(self, position):
        return position.price

    def get_kline(self, symbol, interval, _from):
        counter = 0
        kline = []
        while bt.num2date(self.datas[0].datetime[counter]).strftime('%H:%M:%S') != _from:
            counter -= 1
        while counter != 1:
            kline.append({
                "timestamp": bt.num2date(self.datas[0].datetime[counter]).strftime('%Y-%m-%d %H:%M:%S'),
                "close": self.datas[0].close[counter],
                "high": self.datas[0].high[counter],
                "low": self.datas[0].low[counter],
                "volume": self.datas[0].volume[counter]
            })
            counter += 1
        return kline

    def get_last_kline(self, symbol, interval):
        kline = {
            "timestamp": bt.num2date(self.datas[0].datetime[0]).strftime('%Y-%m-%d %H:%M:%S'),
            "close": self.datas[0].close[0],
            "high": self.datas[0].high[0],
            "low": self.datas[0].low[0],
            "volume": self.datas[0].volume[0]
            }
        return kline

    def edit_stop(self, symbol, stop, p_r_qty, p_r_trigger_price):
        self.orders.remove(stop)
        self.cancel(stop)
        self.orders.insert(0, self.create_stop(symbol, p_r_trigger_price))

    def edit_orders_price(self, symbol, order, price):
        last_price = self.get_last_price_close(symbol)
        order = self.orders.pop()
        self.cancel(order)
        amount = self.config["OTHER"]["Amount"]
        if last_price > price:
            self.orders.append(self.limit_order(symbol, "Buy", amount, price))
        else:
            self.orders.append(self.limit_order(symbol, "Sell", amount, price))

    def get_big_deal(self, symbol):
        with open('big_deals.backtest', 'rb') as bd:
            signals = pickle.load(bd)
        for signal in signals:
            if signal['timestamp'] == bt.num2date(self.datas[0].datetime[0]).strftime('%Y-%m-%d %H:%M:%S'):
                return signal
        return False

    def get_order_book(self, symbol):
        return self.bybit.Market.Market_orderbook(symbol=symbol).result()[0]['result']

    def get_last_price_close(self, symbol):
        return self.datas[0].close[0]

    def get_stop_order(self):
        return self.orders[0]

    def get_time_open(self):
        return self.day_open_dict[self.get_month()]

    def cancel_all_orders(self, symbol):
        for order in self.orders:
            self.cancel(order)
        self.orders = []

    def limit_order(self, symbol, side, amount, price=False):
        if not price:
            limit_price = self.get_limit_price(symbol, side)
        else:
            limit_price = price
        amount = int(amount)
        return self.create_order('Limit', symbol, side, amount, limit_price)

    def get_all_orders(self, symbol):
        return self.orders

    def create_stop(self, symbol, stop_px):
        position = self.true_get_position(symbol)
        base_price = position.price
        if position.size > 0:
            operation = self.sell
            side_scelar = -1
        else:
            operation = self.buy
            side_scelar = 1
        amount = abs(position.size)
        if '%' in stop_px:
            stop_px = base_price + side_scelar * float(stop_px.replace('%', ''))*base_price/100
        elif '$' in stop_px:
            stop_px = base_price + side_scelar * int(stop_px.replace('$', ''))/amount * base_price
        stop_px = int(stop_px)
        self.logger.info("Sending a Create Stop command stop: {} size: {}".format(stop_px, amount))
        s = operation(exectype=bt.Order.Stop, size=amount, price=stop_px)
        return s
