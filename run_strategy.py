import backtrader as bt
import datetime
import sys
from vwap_extreme_points_strategy import VwapExtremePointsStrategy
from vwap_strategy import VwapStrategy
from vwap_extreme_points_gatherer import VwapExtremePointsGatherer
from vwap_combined_strategies import VwapCombinedStrategy
from liquidations_strategy import LiquidationStrategy

strategy = {'vwap': VwapStrategy,
            'vwap_extreme_points': VwapExtremePointsStrategy,
            'vwap_extreme_points_gatherer': VwapExtremePointsGatherer,
            'vwap_combined': VwapCombinedStrategy,
            'liquidations': LiquidationStrategy
            }
if '--Test' in sys.argv:
    data = bt.feeds.GenericCSVData(
        dataname='BTC_1m.csv',
        datetime=0,
        high=2,
        close=4,
        open=1,
        volume=5,
        dateformat='%Y-%m-%d %H:%M:%S',
        fromdate=datetime.datetime(2021, 1, 17, 2, 0, 0),
        #todate=datetime.datetime(2021, 1, 18, 2, 33, 0),
        timeframe=bt.TimeFrame.Minutes,
        compression=1)
    cerebro = bt.Cerebro()
    cerebro.adddata(data)
    #  cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_cash(200000)
    cerebro.addstrategy(strategy[sys.argv[1]])
    print("Start Value {}".format(cerebro.broker.getvalue()))
    cerebro.run()
    print("End Value {}".format(cerebro.broker.getvalue()))
else:
    strategy = strategy[sys.argv[1]]()
    strategy.next()
