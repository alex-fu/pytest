# coding=utf8

import tushare as ts
# import couchbase as cb

def get_stock_list():
    return ts.get_stock_basics()


def store_stock_list(conn):
    stock_list = get_stock_list()
    for index, row in stock_list.iterrows():
        print(index)
        print(row['name'])
        store_stock(index, row)

if __name__ == '__main__':
    with cb.open() as conn:
        store_stock_list(conn)

