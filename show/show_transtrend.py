# coding=utf8
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from flask import Flask
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful import reqparse

from strategy.config import ST_DBURL, STRATEGY_BUCKET_NAME, BUCKET_NAME
from fn import _
from functools import lru_cache
import threading
import schedule

app = Flask(__name__)
CORS(app, supports_credentials=True)
api = Api(app)
cb = Bucket(ST_DBURL)

STNAME = 'transtrend'
STVER = 'v2'


class IndexAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('year', type=str, location='args')
        self.reqparse.add_argument('index', type=str, location='args')

    def get(self):
        args = self.reqparse.parse_args()
        year = args['year']
        index = args['index']

        # closes = None
        if year and int(year) in range(2011, 2017):
            closes = get_index_close_by_year(year, index)
        else:
            closes = get_index_close_all(index)

        # r = map(lambda x: {'date': x, 'close': closes[x]}, sorted(closes.keys()))
        return {'result': list(closes)}


@lru_cache(maxsize=32)
def get_index_close_by_year(year, index):
    from strategy.dbutils import get_index_day_kline
    r = get_index_day_kline(index, year + '-01-01', year + '-12-31')

    return list(map(lambda x: {'date': x['date'], 'close': x['close']}, r))


def get_index_close_all(index):
    r2011 = get_index_close_by_year('2011', index)
    r2012 = get_index_close_by_year('2012', index)
    r2013 = get_index_close_by_year('2013', index)
    r2014 = get_index_close_by_year('2014', index)
    r2015 = get_index_close_by_year('2015', index)
    r2016 = get_index_close_by_year('2016', index)
    return r2011 + r2012 + r2013 + r2014 + r2015 + r2016


class TranstrendWinRateAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('year', type=str, location='args')
        self.reqparse.add_argument('ndayclose', type=int, location='args')

    def get(self):
        args = self.reqparse.parse_args()
        year = args['year']
        ndayclose = args['ndayclose']

        trades = None
        if year and int(year) in range(2011, 2017):
            trades = get_trades_by_year(year, ndayclose)
        else:
            trades = get_trades_all(ndayclose)

        r = map(lambda x: {'date': x, 'tradenum': len(trades[x]), 'winrate': get_win_rate(trades[x])},
                sorted(trades.keys()))
        return {'result': list(r)}


class TranstrendWinRateDateRangeAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('startdate', type=str, location='args')
        self.reqparse.add_argument('enddate', type=str, location='args')
        self.reqparse.add_argument('ndayclose', type=int, required=True, location='args')

    def get(self):
        args = self.reqparse.parse_args()

        try:
            # check date format
            from strategy.utils import check_date
            check_date(args['startdate'])
            check_date(args['enddate'])
            startdate = args['startdate']
            enddate = args['enddate']
            if startdate < '2011-01-01':
                startdate = '2011-01-01'
            elif enddate < startdate:
                enddate = startdate
        except ValueError:
            print("ERROR: unrecognized date format! Should be like 2010-11-11")
            return {'result': "Invalid date format on startdate or enddate. e.g. 2016-02-01"}

        startyear = int(startdate[:4])
        endyear = int(enddate[:4])
        ndayclose = args['ndayclose']

        trade_list = []
        for year in range(startyear, endyear + 1):
            trades = get_trades_by_year(str(year), ndayclose)
            for date, trade in trades.items():
                if startdate <= date <= enddate:
                    trade_list += trade

        return {'result': {'startdate': startdate, 'enddate': enddate, 'tradenum': len(trade_list),
                           'winrate': get_win_rate(trade_list)}}


class TranstrendAPI(Resource):
    def __init__(self):
        self.reqparse = reqparse.RequestParser()
        self.reqparse.add_argument('year', type=str, location='args')
        self.reqparse.add_argument('ndayclose', type=int, location='args')

    def get(self):
        args = self.reqparse.parse_args()
        year = args['year']
        ndayclose = args['ndayclose']

        yields = None
        if year and int(year) in range(2011, 2017):
            yields = get_yield_by_year(year, ndayclose)
        else:
            yields = get_yield_all(ndayclose)

        r = map(lambda x: {'date': x, 'rate': yields[x]}, sorted(yields.keys()))
        return {'result': list(r)}


def get_trades_all(ndayclose):
    r2011 = get_trades_by_year('2011', ndayclose)
    r2012 = get_trades_by_year('2012', ndayclose)
    r2013 = get_trades_by_year('2013', ndayclose)
    r2014 = get_trades_by_year('2014', ndayclose)
    r2015 = get_trades_by_year('2015', ndayclose)
    r2016 = get_trades_by_year('2016', ndayclose)
    return {**r2011, **r2012, **r2013, **r2014, **r2015, **r2016}


def get_yield_all(ndayclose):
    r2011 = get_yield_by_year('2011', ndayclose)
    r2012 = get_yield_by_year('2012', ndayclose)
    r2013 = get_yield_by_year('2013', ndayclose)
    r2014 = get_yield_by_year('2014', ndayclose)
    r2015 = get_yield_by_year('2015', ndayclose)
    r2016 = get_yield_by_year('2016', ndayclose)
    return {**r2011, **r2012, **r2013, **r2014, **r2015, **r2016}


@lru_cache(maxsize=32)
def get_trades_by_year(year, ndayclose):
    cql = 'SELECT trades FROM `' + STRATEGY_BUCKET_NAME + \
          '` WHERE strategy_name=$stname and strategy_version=$stver' + \
          ' and startdate=$startdate and enddate=$enddate and ndayclose=$ndayclose'
    r = cb.n1ql_query(N1QLQuery(cql, stname=STNAME, stver=STVER, startdate=year + '-01-01', enddate=year + '-12-31',
                                ndayclose=ndayclose))
    r1 = {}
    for x in r:
        trades = x['trades']
        for trade in trades:
            date = trade['closeDate']
            if date is None:
                continue
            if date not in r1:
                r1[date] = [trade]
            else:
                r1[date].append(trade)
    return r1


@lru_cache(maxsize=32)
def get_yield_by_year(year, ndayclose):
    r = get_trades_by_year(year, ndayclose)

    r1 = {}
    for date, trades in r.items():
        opened = 0.0
        closed = 0.0
        for trade in trades:
            opened += trade['openPrice']
            closed += trade['closePrice']
        r1[date] = (closed - opened) / opened

    return r1


def get_win_rate(trade_group):
    win = 0
    for trade in trade_group:
        if trade['closePrice'] - trade['openPrice'] > 0:
            win += 1
    return win / len(trade_group)


def check_lru_cache():
    print(get_yield_by_year.cache_info())
    print(get_index_close_by_year.cache_info())


class MonitorThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        schedule.every(1).minutes.do(check_lru_cache)
        print("Monitor thread inited!")

    def run(self):
        print("Monitor thread run!")
        while True:
            import time
            schedule.run_pending()
            time.sleep(1)


def main():
    t = MonitorThread()
    t.start()

    api.add_resource(TranstrendAPI, '/transtrend', endpoint='transtrend')
    api.add_resource(TranstrendWinRateAPI, '/transtrend/winrate', endpoint='transtrend_winrate')
    api.add_resource(TranstrendWinRateDateRangeAPI, '/transtrend/winrate/daterange',
                     endpoint='transtrend_winrate_daterange')
    api.add_resource(IndexAPI, '/index', endpoint='index')
    app.run(host='192.168.66.211', debug=True)

    t.join()


if __name__ == '__main__':
    main()
    # print(list(get_index_close_by_year('2016', '000002')))
    # print(list(get_index_close_all('000300')))
