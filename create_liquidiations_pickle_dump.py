import pickle
import datetime
import bybit_tools
import os.path

bt = bybit_tools.BybitTools()
if os.path.isfile('liq_occupied'):
    print("Pickle File is occupied, Exiting.")
    exit()

with open('liquidations', 'rb') as lq:
    liqs = pickle.load(lq)

new_liqs = bt.bybit.Market.Market_liqRecords(symbol="BTCUSD", limit=1000).result()[0]['result']
place = 0
for tt in new_liqs:
    if tt in liqs:
        continue
    liqs.insert(place, tt)
    place += 1

bt.update_buy_sell_thresh_hold(liqs, 4, 15)
bt.update_liqs_factor(liqs, 4, 15)
print('{} liqs: {}, liq_factor: {}, liq overall factor: {}, weighted factor: {}'.format(
    bt.get_date(), len(liqs), bt.liqs_factor, bt.liqs_overall_power_ratio, bt.liqs_weighted_ratio))
with open('liquidations', 'wb') as lq:
    pickle.dump(liqs, lq)

liq_1m_dict = dict()
liq_1m_list = list()
for x in liqs:
    x['time'] = datetime.datetime.fromtimestamp(int(x['time']/1000)).strftime("%d/%m/%Y, %H:%M")
    if not x['time'] in liq_1m_dict.keys():
        liq_1m_dict[x['time']] = {"Buy": 0, "Sell": 0}
    liq_1m_dict[x['time']][x['side']] += x['qty']
place = 0
with open('liquidations_1m', 'wb') as lq:
    pickle.dump(liq_1m_dict, lq)
