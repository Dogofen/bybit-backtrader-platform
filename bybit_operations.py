import bybit
import configparser
import datetime
from time import sleep
from botlogger import Logger


class BybitOperations(object):

    API_KEY = ''
    liquidations_dict = {}
    API_SECRET = ''
    liq_1m_dict = {}
    liquidations = []
    orders = []
    logger = ''
    bybit = ''
    env = ''
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
        '12': "02:00:00"
    }

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('conf.ini')
        self.API_KEY = self.config['API_KEYS']['api_key']
        self.API_SECRET = self.config['API_KEYS']['api_secret']
        self.interval = self.config["Vwap"]["Interval"]
        self.env = self.config['OTHER']['env']
        bot_logger = Logger()
        self.logger = bot_logger.init_logger()

        if self.env == 'test':
            test = True
        else:
            test = False
        self.bybit = bybit.bybit(test=test, api_key=self.API_KEY, api_secret=self.API_SECRET)
        self.logger.info("Finished BybitTools construction, proceeding")

    @staticmethod
    def get_date():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_month():
        return datetime.datetime.now().strftime('%m')

    @staticmethod
    def get_datetime():
        return datetime.datetime.now()

    @staticmethod
    def liq_current_time_no_seconds(date_time=False):
        if date_time:
            dt = datetime.datetime.now()
            return dt - datetime.timedelta(minutes=1, seconds=dt.second)
        return (datetime.datetime.now()-datetime.timedelta(minutes=1)).strftime('%d/%m/%Y, %H:%M')

    @staticmethod
    def get_position_size(position):
        return position['size']

    @staticmethod
    def get_position_side(position):
        return position['side']

    @staticmethod
    def get_position_price(position):
        return float(position['entry_price'])

    @staticmethod
    def get_start_date():
        return datetime.datetime.now()

    def edit_orders_price(self, symbol, order_id, price):
        order_id = order_id['order_id']
        self.logger.info("editing order:{} price:{}.".format(order_id, price))
        self.bybit.Order.Order_replace(symbol=symbol, order_id=order_id, p_r_price=str(price)).result()

    def get_liquidations(self, symbol):
        liquidations = False
        fault_counter = 0
        while liquidations is False:
            if fault_counter > 5:
                self.logger.error("Get liquidations reached it's max tries")
                break
            try:
                liquidations = self.bybit.Market.Market_liqRecords(symbol=symbol, limit=1000).result()[0]['result']
            except Exception as e:
                self.logger.error("Get liquidations has failed {}".format(e))
                liquidations = False
                fault_counter += 1
        return liquidations

    def update_liquidations(self, symbol):
        liqs = self.get_liquidations(symbol)
        place = 0
        for liq in liqs:
            if liq in self.liquidations:
                continue
            self.liquidations.insert(place, liq)
            place += 1
        while len(self.liquidations) > 1500:
            self.liquidations.pop()

    def get_last_kline(self, symbol, interval):
        _from = int((datetime.datetime.now() - datetime.timedelta(minutes=int(interval) * 4)).timestamp())
        kline_time = self.liq_current_time_no_seconds(True)
        kline = []
        attempts = 0
        while attempts < 10:
            try:
                kline = self.get_kline(symbol, interval, _from)
                for k in kline:
                    if k['open_time'] == int(kline_time.timestamp()):
                        return k
                attempts += 1
                sleep(2)
            except Exception as e:
                self.logger.error("get last Kline has failed: {} kline returned was: {}".format(e, kline))
                sleep(2)
        self.logger.error("get last Kline has failed, attempts reached the limit: {}".format(attempts))
        return False

    def update_minute_liquidations_dict(self):
        liq_1m_dict = dict()
        for liq in self.liquidations:
            time = datetime.datetime.fromtimestamp(int(liq['time'] / 1000)).strftime("%d/%m/%Y, %H:%M")
            if time not in liq_1m_dict.keys():
                liq_1m_dict[time] = {"Buy": 0, "Sell": 0}
            liq_1m_dict[time][liq['side']] += liq['qty']
        self.liq_1m_dict = liq_1m_dict

    def get_stop_order(self):
        return self.orders[0]

    def get_time_open(self):
        return self.day_open_dict[self.get_month()]

    def get_big_deal(self, symbol):
        bd = False
        try:
            bd = self.bybit.Market.Market_bigDeal(symbol=symbol).result()[0]['result'][0]
        except Exception as e:
            self.logger.error("Getting big deal Failed {}".format(e))
        return bd

    def get_cash(self, coin):
        return self.bybit.Wallet.Wallet_getBalance(coin=coin).result()[0]['result'][coin]['wallet_balance']

    def get_current_liquidations_dict(self, from_time_in_minutes):
        liquidation_dict = {}
        now = datetime.datetime.strptime(self.liq_current_time_no_seconds(), '%d/%m/%Y, %H:%M')
        self.update_minute_liquidations_dict()
        for k in self.liq_1m_dict.keys():
            if datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M') > now:
                continue
            if datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M') >= from_time_in_minutes:
                liquidation_dict[k] = self.liq_1m_dict[k]
        return liquidation_dict

    def edit_stop(self, symbol, stop_id, p_r_qty, p_r_trigger_price):
        try:
            stop_id = stop_id['stop_order_id']
            self.bybit.Conditional.Conditional_replace(
                symbol=symbol,
                stop_order_id=stop_id,
                p_r_qty=str(p_r_qty),
                p_r_trigger_price=str(p_r_trigger_price)
            ).result()
        except Exception as e:
            self.logger.error("edit stop order Failed {}".format(e))

    def create_order(self, order_type, symbol, side, amount, price):
        price = int(price)
        order = False
        order_status = False
        self.logger.info(
            "Sending a Create Order command type => {} side =>{} amount=>{} price=>{}".format(
                order_type,
                side,
                amount,
                price)
        )
        while order_status != 'Created':
            try:
                order = self.bybit.Order.Order_new(
                    side=side,
                    symbol=symbol,
                    order_type=order_type,
                    qty=amount,
                    price=price,
                    time_in_force="GoodTillCancel"
                ).result()[0]['result']
                order_status = order['order_status']
            except Exception as e:
                self.logger.error("Create Trade Failed {}".format(e))
                order_status = False
                sleep(3)
        return order

    def return_datetime_from_liq_dict(self, value, side):
        for k in self.liquidations_dict.keys():
            if self.liquidations_dict[k][side] == value:
                return datetime.datetime.strptime(k, '%d/%m/%Y, %H:%M') + datetime.timedelta(minutes=1)
        return False

    def cancel_order(self, symbol, order):
        order_id = order['order_id']
        self.bybit.Order.Order_cancel(symbol=symbol, order_id=order_id).result()
        self.orders.remove(order)

    def get_kline(self, symbol, interval, _from):
        kline = False
        fault_counter = 0
        last_k = False
        while not last_k:
            if fault_counter > 10:
                self.logger.error("Kline Failed to retrieved fault counter has {} tries".format(fault_counter))
                break
            try:
                kline = self.bybit.Kline.Kline_get(
                    symbol=symbol, interval=interval, **{'from': _from}
                ).result()[0]['result']
                last_k = kline[-1]
            except Exception as e:
                self.logger.error("get Kline returned: {} error was: {}".format(kline, e))
                last_k = False
                sleep(2)
            fault_counter += 1
            sleep(1)
        for cc in kline:
            for key in cc.keys():
                if key == 'high' or key == 'low' or key == 'open' or key == 'close' or key == 'volume':
                    cc[key] = float(cc[key])
        return kline

    def get_last_price_close(self, symbol):
        kline = False
        try:
            kline = self.get_kline(
                symbol,
                self.interval,
                (datetime.datetime.now() - datetime.timedelta(minutes=int(self.interval))).timestamp()
            )
        except Exception as e:
            self.logger.error("get Kline returned: {} error was: {}".format(kline, e))
        try:
            return int(float(kline[0]['close']))
        except Exception as e:
            self.logger.error("Get last kline has failed: {}".format(e))
            return False

    def true_cancel_stop(self, symbol):
        fault_counter = 0
        while len(self.true_get_stop_order(symbol)) != 0:
            if fault_counter > 5:
                self.logger.error(
                    "Cancel stop Failed, fault counter has {} tries".format(fault_counter)
                )
                break
            try:
                self.logger.info(self.bybit.Conditional.Conditional_cancelAll(symbol=symbol).result())
            except Exception as e:
                self.logger.error("Cancel Stop returned: {}".format(e))
                sleep(2)
            fault_counter += 1
            sleep(1)

    def true_cancel_orders(self, symbol):
        fault_counter = 0
        while len(self.true_get_active_orders(symbol)) != 0:
            if fault_counter > 5:
                self.logger.error(
                    "Cancel orders Failed, fault counter has {} tries".format(fault_counter)
                )
                break
            try:
                self.logger.info(self.bybit.Order.Order_cancelAll(symbol=symbol).result())
            except Exception as e:
                self.logger.error("Cancel orders returned: {}".format(e))
                sleep(2)
            fault_counter += 1
            sleep(1)

    def get_time_delta(self, count):
        return float(int(self.get_datetime().timestamp()) - 60 * count)

    def cancel_all_orders(self, symbol):
        if len(self.true_get_stop_order(symbol)) != 0:
            try:
                self.true_cancel_stop(symbol)
            except Exception as e:
                self.logger.error("Failed cancelling Orders {}".format(e))
                return
        if len(self.true_get_active_orders(symbol)) != 0:
            try:
                self.true_cancel_orders(symbol)
            except Exception as e:
                self.logger.error("Failed cancelling Orders {}".format(e))
                return
        self.logger.info("All Orders been cancelled")
        self.orders = []
        return True

    def true_get_position(self, symbol):
        position = False
        rate_limit_status = False
        fault_counter = 0
        size = 0
        while position is False:
            if fault_counter > 10:
                self.logger.error("position Failed to retrieved fault counter has {} tries".format(fault_counter))
                break
            position = self.bybit.Positions.Positions_myPosition(symbol=symbol).result()[0]
            try:
                rate_limit_status = position['rate_limit_status']
                size = position['result']['size']
            except Exception as e:
                self.logger.error("get position returned: {} error was: {} size: {}".format(position, e, size))
                self.logger.info("self rate limit: {}".format(rate_limit_status))
                position = False
                sleep(2)

            fault_counter += 1
            sleep(1)
        return position['result']

    def true_get_active_orders(self, symbol):
        success = False
        fault_counter = 0
        orders = False
        while success is False:
            if fault_counter > 5:
                self.logger.error(
                    "Get Active Orders Failed to retrieved fault counter has {} tries".format(fault_counter)
                )
            try:
                orders = self.bybit.Order.Order_getOrders(
                    symbol=symbol, order_status="New"
                ).result()[0]['result']['data']
                success = True
            except Exception as e:
                self.logger.error("get active orders returned: {} error was: {}".format(orders, e))
                success = False
                fault_counter += 1
                sleep(2)
        return orders

    def true_get_stop_order(self, symbol):
        success = False
        fault_counter = 0
        stop_order = False
        while success is False:
            if fault_counter > 5:
                self.logger.error(
                    "Get Active Orders Failed to retrieved fault counter has {} tries".format(fault_counter)
                )
            try:
                stop_order = self.bybit.Conditional.Conditional_getOrders(
                    symbol=symbol, stop_order_status="Untriggered"
                ).result()[0]['result']['data']
                success = True
            except Exception as e:
                self.logger.error("get active orders returned: {} error was: {}".format(stop_order, e))
                success = False
                fault_counter += 1
                sleep(2)
        return stop_order

    def is_open_position(self, symbol):
        open_position = self.true_get_position(symbol)
        if open_position['size'] == 0:
            return False
        return open_position

    def get_order_book(self, symbol):
        return self.bybit.Market.Market_orderbook(symbol=symbol).result()[0]['result']

    def get_limit_price(self, symbol, side):
        ob = self.get_order_book(symbol)
        return ob[side][0]

    def limit_order(self, symbol, side, amount, price=False):
        if price is False:
            limit_price = self.get_limit_price(symbol, side)
        else:
            limit_price = price
        return self.create_order('Limit', symbol, side, amount, limit_price)

    def create_stop(self, symbol, stop_px):
        position = self.true_get_position(symbol)
        base_price = int(float(position['entry_price']))
        if position['side'] == 'Buy':
            side = 'Sell'
            side_scelar = -1
        else:
            side = 'Buy'
            side_scelar = 1
        amount = int(position['size'])
        if '%' in stop_px:
            stop_px = base_price + side_scelar * float(stop_px.replace('%', ''))*base_price/100
        elif '$' in stop_px:
            stop_px = base_price + side_scelar * int(stop_px.replace('$', ''))/amount * base_price
        stop_px = int(stop_px)
        stop_px = str(stop_px)
        amount = str(amount)
        base_price = str(base_price)
        stop = None
        while stop is None:
            try:
                stop = self.bybit.Conditional.Conditional_new(
                    order_type="Market",
                    side=side,
                    symbol=symbol,
                    qty=amount,
                    stop_px=stop_px,
                    base_price=base_price,
                    time_in_force="GoodTillCancel"
                ).result()
                stop = stop[0]['result']
            except Exception as e:
                self.logger.error("Stop Failed to be created {}".format(e))
                stop = None
        self.logger.info("Sending a Create Stop command side =>{} stop =>{}".format(side, stop_px))
        self.logger.info("Command's result: {}".format(stop))
        return stop
