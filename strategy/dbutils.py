# coding = utf8

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery

from strategy.config import *


def get_stocklist_fromdb():
    cb = Bucket(DBURL)

    cql = 'SELECT code FROM `' + BUCKET_NAME + '` WHERE type="stock"'
    r = cb.n1ql_query(N1QLQuery(cql))
    return map(lambda x: x['code'], r)


def get_day_kline(stock, startdate, enddate):
    cb = Bucket(DBURL)

    cql = 'SELECT * FROM `' + BUCKET_NAME + \
          '` WHERE type="stock_kday" and code=$code and date>$startdate and date<$enddate ORDER BY date'
    r = cb.n1ql_query(N1QLQuery(cql, code=stock, startdate=startdate, enddate=enddate))
    return map(lambda x: x['stock'], r)


if __name__ == '__main__':
    # print(list(get_stocklist_fromdb()))
    print(list(get_day_kline('000777', '2016-01-01', '2016-12-31')))
