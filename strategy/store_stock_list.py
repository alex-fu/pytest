# coding=utf8

import tushare as ts
from couchbase.bucket import Bucket
from couchbase.exceptions import CouchbaseError

from strategy.config import *

G_saved = 0
G_failed = 0


def get_stock_list():
    print("Getting stock list...")
    return ts.get_stock_basics()


def fill_stock_info_from_ts(index, row):
    d = {'code': index,
         'type': 'stock'}
    for key, value in row.iteritems():
        d[key] = value
    return d


def store_stock(bucket, index, row):
    global G_saved, G_failed
    # print("Store {}".format(index))
    try:
        rv = bucket.upsert(STOCK_PREFIX + index, fill_stock_info_from_ts(index, row))
        if not rv.success:
            print("ERROR: upsert stock list failed!")
            G_failed += 1
        else:
            G_saved += 1
    except CouchbaseError as e:
        G_failed += 1
        print('-'*30)
        print("Exception:", e)
        # import traceback
        # traceback.print_exc()
        print('-'*30)


def store_stock_list(bucket):
    stock_list = get_stock_list()
    for index, row in stock_list.iterrows():
        store_stock(bucket, index, row)
    print("Total saved: {}, total failed: {}".format(G_saved, G_failed))


def main():
    """
    Intention:
        Store the stock list to DB
    Data structure in DB:
        stock_<code>: { attr of stock }
    Example:
        stock_000001: {
          "area": "深圳",
          "timeToMarket": 19910403,
          "reservedPerShare": 3.29,
          "code": "000001",
          "totals": 1717041.13,
          "liquidAssets": 0,
          "pb": 0.89,
          "esp": 1.09,
          "pe": 6.34,
          "industry": "银行",
          "outstanding": 1463118,
          "bvps": 10.38,
          "name": "平安银行",
          "fixedAssets": 740700,
          "reserved": 5646500,
          "totalAssets": 279123808
        }
    """
    print("Start to store stock list...")
    bucket = Bucket(DBURL)
    store_stock_list(bucket)
    print("Store stock list done!")


if __name__ == '__main__':
    main()
