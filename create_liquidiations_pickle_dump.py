import configparser
import pickle
import datetime
import bybit
import os.path

if os.path.isfile('liq_occupied'):
    print("Pickle File is occupied, Exiting.")
    exit()

with open('liquidations', 'rb') as lq:
    liqs = pickle.load(lq)
config = configparser.ConfigParser()
config.read('conf.ini')

API_KEY = config['API_KEYS']['api_key']
API_SECRET = config['API_KEYS']['api_secret']

client = bybit.bybit(test=False, api_key=API_KEY, api_secret=API_SECRET)
new_liqs = client.Market.Market_liqRecords(symbol="BTCUSD", limit=1000).result()[0]['result']
place = 0
for tt in new_liqs:
    if tt in liqs:
        continue
    liqs.insert(place, tt)
    place += 1
print(len(liqs))
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
