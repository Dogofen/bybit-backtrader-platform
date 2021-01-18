import backtrader as bt
import configparser
import datetime
from botlogger import Logger
import pickle


class BybitOperations(bt.Strategy):
    logger = ''
    orders = []
    order = ''
    liqs = []
    liqs_1m = {}
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
        with open('liquidations_1m', 'rb') as lq:
            self.liqs_1m = pickle.load(lq)
        with open('liquidations', 'rb') as lq:
            self.liqs = pickle.load(lq)

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

    def get_current_liquidations_dict(self, symbol, from_time_in_minutes):
        liquidation_dict = {}
        now = datetime.datetime.strptime(self.liq_current_time_no_seconds(), '%d/%m/%Y, %H:%M')
        for k in self.liqs_1m.keys():
            if datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M') > now:
                continue
            if datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M') >= from_time_in_minutes:
                liquidation_dict[k] = self.liqs_1m[k]
        return liquidation_dict

    def get_distance_from_vwap(self, last_price, vwap):
        distance = abs(last_price - vwap)
        return distance / last_price

    def limit_close_trade(self, symbol):
        position = self.true_get_position(symbol)
        position_size = self.get_position_size(position)
        if position_size > 0:
            self.sell(size=abs(position_size))
        else:
            self.buy(size=abs(position_size))

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

    def liq_current_time_no_seconds(self):
        return bt.num2date(self.datas[0].datetime[0]).strftime('%d/%m/%Y, %H:%M')

    def get_position_price(self, position):
        return position.price

    def get_time_delta(self, count):
        _from = float(int(self.get_datetime().timestamp()) - 60 * count)
        if bt.num2date(self.datas[0].fromdate).timestamp() > _from:
            return bt.num2date(self.datas[0].fromdate).timestamp()
        return _from

    def get_kline(self, symbol, interval, _from):
        counter = 0
        kline = []
        while bt.num2date(self.datas[0].datetime[counter]).timestamp() > _from:
            if counter > 1440:
                break
            counter -= 1
        while counter != 1:
            kline.append({
                "timestamp": bt.num2date(self.datas[0].datetime[counter]).strftime('%Y-%m-%d %H:%M:%S'),
                "close": self.datas[0].close[counter],
                "open": self.datas[0].open[counter],
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
            "open": self.datas[0].open[0],
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

    def get_datetime(self):
        return bt.num2date(self.datas[0].datetime[0])

    def cancel_all_orders(self, symbol):
        for order in self.orders:
            self.cancel(order)
        self.orders = []

    def cancel_order(self, symbol, order):
        self.logger.info("Cancelling order: {}".format(order))
        self.cancel(order)
        self.orders.remove(order)

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
