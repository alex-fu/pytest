# coding=utf8
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from flask import Flask
from flask_restful import Api, Resource

from strategy.config import ST_DBURL, STRATEGY_BUCKET_NAME
from fn import _

app = Flask(__name__)
api = Api(app)
cb = Bucket(ST_DBURL)

STNAME = 'transtrend'
STVER = 'v1'


class TranstrendAPI(Resource):
    def get(self):
        trades = get_trades_2016_all()
        r = map(lambda x: {'date': x, 'rate': trades[x]}, trades)

        return {'result': list(r)}


def get_trades_2016_all():
    cql = 'SELECT trades FROM `' + STRATEGY_BUCKET_NAME + \
          '` WHERE strategy_name=$stname and strategy_version=$stver' + \
          ' and startdate=$startdate and enddate=$enddate'
    r = cb.n1ql_query(N1QLQuery(cql, stname=STNAME, stver=STVER, startdate='2016-01-01', enddate='2016-12-31'))
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


if __name__ == '__main__':
    api.add_resource(TranstrendAPI, '/transtrend', endpoint='transtrend')
    app.run(debug=True)
