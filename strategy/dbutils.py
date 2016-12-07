# coding = utf8

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery

from strategy.config import *

from fn import _

from retry import retry


@retry(TimeoutError, tries=3)
def get_stocklist_fromdb():
    cb = Bucket(DBURL)

    cql = 'SELECT code FROM `' + BUCKET_NAME + '` WHERE type="stock"'
    r = cb.n1ql_query(N1QLQuery(cql))
    return map(lambda x: x['code'], r)


@retry(TimeoutError, tries=3)
def get_indexlist_fromdb():
    cb = Bucket(DBURL)

    cql = 'SELECT code FROM `' + BUCKET_NAME + '` WHERE type="index"'
    r = cb.n1ql_query(N1QLQuery(cql))
    return map(lambda x: x['code'], r)


@retry(TimeoutError, tries=3)
def get_day_kline(stock, startdate, enddate):
    cb = Bucket(DBURL)

    cql = 'SELECT * FROM `' + BUCKET_NAME + \
          '` WHERE type="stock_kday" and code=$code and date>$startdate and date<$enddate ORDER BY date'
    r = cb.n1ql_query(N1QLQuery(cql, code=stock, startdate=startdate, enddate=enddate))
    return map(_['stock'], r)
    # return map(lambda x: x['stock'], r)


@retry(TimeoutError, tries=3)
def get_index_day_kline(index, startdate, enddate):
    cb = Bucket(DBURL)

    cql = 'SELECT * FROM `' + BUCKET_NAME + \
          '` WHERE type="index_kday" and code=$code and date>$startdate and date<$enddate ORDER BY date'
    r = cb.n1ql_query(N1QLQuery(cql, code='index_'+index, startdate=startdate, enddate=enddate))
    return map(_['stock'], r)
    # return map(lambda x: x['stock'], r)


if __name__ == '__main__':
    print(list(get_stocklist_fromdb()))
    # print(list(get_day_kline('000779', '2016-01-01', '2016-12-31')))
