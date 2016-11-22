# coding=utf8
from strategy.utils import Bean


def toKLineMap(klines):
    """
    kline structure:
    {
      "volume": 35884060,
      "type": "stock_kday",
      "code": "000001",
      "amount": 573343744,
      "date": "2011-01-04",
      "close": 9.43,
      "open": 9.32,
      "fqtype": "qfq",
      "low": 9.22,
      "high": 9.53
    }
    :return:
        dict { date => kline }
    """
    d = dict()
    for k in klines:
        d[k.date] = k
    return d


class Trade(object):
    def __init__(self):
        self.open_date = None
        self.open_price = None
        self.close_date = None
        self.close_price = None

    def json(self):
        return {
            'openDate': self.open_date,
            'openPrice': self.open_price,
            'closeDate': self.close_date,
            'closePrice': self.close_price
        }


class Position(Bean):
    def __init__(self):
        self.volume = 0
        self.open_price = None


class TestLine(Bean):
    def __init__(self):
        self._trades = []
        self._position = Position()
        self.stock = None
        self.startdate = None
        self.enddate = None
        self.klines = []
        self.klineIndex = 0

    def json(self):
        return {
            'stock': self.stock,
            'startdate': self.startdate,
            'enddate': self.enddate
        }

    def getk(self):
        assert(self.klineIndex >= 0)
        assert(len(self.klines) > self.klineIndex)
        return self.klines[self.klineIndex]

    def open1(self, price):
        assert(self.klineIndex >= 0)
        assert(len(self.klines) > self.klineIndex)
        # manage trade
        if len(self._trades) > 0:
            trade = self._trades[-1]
            if trade.close_price is None or trade.close_date is None:
                print('ERROR: Did not closed trade on {} with {} for {}'.format(trade.open_date, trade.open_price,
                                                                                self.stock))
                return
        trade = Trade()
        trade.open_date = self.klines[self.klineIndex]['date']
        trade.open_price = price
        self._trades.append(trade)
        # manage position
        if self._position.volume > 0 or self._position.open_price is not None:
            print('ERROR: Did not closed position[vol: {}, open_price: {}]'.format(self._position.volume,
                                                                                  self._position.open_price))
            return
        self._position.volume = 1
        self._position.open_price = price
        self._position.klineIndex = self.klineIndex

    def close1(self, price):
        assert(self.klineIndex >= 0)
        assert(len(self.klines) > self.klineIndex)
        # manage trade
        trade = self._trades[-1]
        if trade.close_price is not None or trade.close_date is not None:
            print('ERROR: Already closed trade on {} with {} for {}'.format(trade.close_date, trade.close_price,
                                                                            self.stock))
            return
        trade.close_date = self.klines[self.klineIndex]['date']
        trade.close_price = price
        # manage position
        if self._position.volume == 0 or self._position.open_price is None:
            print('ERROR: No position to close!')
            return
        self._position.volume = 0
        self._position.open_price = None

    def position1(self):
        return self._position

    def trades(self):
        return self._trades
