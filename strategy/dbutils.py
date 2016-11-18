# coding = utf8

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery

from strategy.config import *


def get_stocklist_fromdb():
    cb = Bucket(DBURL)

    cql = 'SELECT code FROM `' + BUCKET_NAME + '` WHERE type=$type'
    r = cb.n1ql_query(N1QLQuery(cql, type='stock'))
    return map(lambda x: x['code'], r)

if __name__ == '__main__':
    print(list(get_stocklist_fromdb()))

