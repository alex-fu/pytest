# coding=utf8
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from flask import Flask
from flask_cors import CORS
from flask_restful import Api, Resource
from flask_restful import reqparse
from functools import wraps
from flask import make_response

from strategy.config import ST_DBURL, STRATEGY_BUCKET_NAME
from fn import _
from functools import lru_cache
import threading
import schedule

app = Flask(__name__)
CORS(app, supports_credentials=True)
api = Api(app)
cb = Bucket(ST_DBURL)

STNAME = 'transtrend'
STVER = 'v1'


class TranstrendAPI(Resource):
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

        r = map(lambda x: {'date': x, 'rate': trades[x]}, sorted(trades.keys()))
        return {'result': list(r)}


def get_trades_all(ndayclose):
    r2011 = get_trades_by_year('2011', ndayclose)
    r2012 = get_trades_by_year('2012', ndayclose)
    r2013 = get_trades_by_year('2013', ndayclose)
    r2014 = get_trades_by_year('2014', ndayclose)
    r2015 = get_trades_by_year('2015', ndayclose)
    r2016 = get_trades_by_year('2016', ndayclose)
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

    # print(r1['2016-08-16'])
    r2 = {}
    for date, trades in r1.items():
        opened = 0.0
        closed = 0.0
        for trade in trades:
            opened += trade['openPrice']
            closed += trade['closePrice']
        r2[date] = (closed - opened) / opened

    return r2


def check_lru_cache():
    print(get_trades_by_year.cache_info())


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


if __name__ == '__main__':
    t = MonitorThread()
    t.start()

    api.add_resource(TranstrendAPI, '/transtrend', endpoint='transtrend')
    app.run(debug=True)

    t.join()
