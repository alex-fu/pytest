# coding=utf8

import tushare as ts
from concurrent import futures

from couchbase.bucket import Bucket, CouchbaseError
from functools import partial

from strategy.config import *

from prometheus_client import Counter

G_retrieve_stock_succeed = Counter('retrieve_stock_succeed', 'Counter of retrieve stock succeed')
G_retrieve_stock_failed = Counter('retrieve_stock_failed', 'Counter of retrieve stock failed')
G_retrieve_klines = Counter('retrieve_klines', 'Counter of retrieved klines')
G_retrieve_db_saved = Counter('retrieve_db_saved', 'Counter of kline saved to db')
G_retrieve_db_failed = Counter('retrieve_db_failed', 'Counter of kline save failed to db')


def cli():
    """
    Object:
        Store day kline for all stocks per year
    Data structure in DB:
        kday_<code>_<date>: {
            'code': xxx,
            'date': xxx,
            'type': kday,
            'fqtype': 'qfq'  (前复权)
            'open': xxx,
            'close': xxx,
            ...
        }
    Example:
        kday_600000_2010-11-11: {
            "date": "2010-11-11",
            "close": 19.08,
            "type": "kday",
            "volume": 140741152,
            "amount": 1997773568,
            "high": 19.36,
            "code": "600000",
            "open": 18.9,
            "low": 18.82,
            "fqtype": "qfq"
        }

    """
    # parse argument
    import argparse
    parser = argparse.ArgumentParser(description='Store day kline')
    parser.add_argument('-s', '--start', required=True,
                        help='please set start date to retrieve&store. Format: "2010-11-11"')
    parser.add_argument('-e', '--end', required=True,
                        help='please set end date. Format: same as --start')
    parser.add_argument('stocks', nargs='*')
    args = parser.parse_args()

    # check date format
    try:
        from strategy.utils import check_date
        check_date(args.start)
        check_date(args.end)
    except ValueError:
        print("ERROR: unrecognized date format! Should be like 2010-11-11")
        exit(1)

    # get date range
    startdate = args.start
    enddate = args.end

    # get stocks
    stocks = args.stocks

    _do_sync_day_kline_multi(stocks, startdate, enddate)


def _do_sync_day_kline_multi(stocks, startdate, enddate):
    if len(stocks) == 0:
        from strategy.dbutils import get_stocklist_fromdb
        stocks = get_stocklist_fromdb()
        print("Try to retrieve & store day kline from {} to {} on all stocks".format(startdate, enddate))
    else:
        print("Try to retrieve & store day kline from {} to {} on {}".format(startdate, enddate, stocks))

    sync_day_kline_multi(stocks, startdate, enddate)
    print("~Completed~")
    print("-"*30)
    print("~Stats~")
    print("G_retrieve_stock_succeed: ", str(G_retrieve_stock_succeed))
    print("G_retrieve_stock_failed: ", str(G_retrieve_stock_failed))
    print("G_retrieve_klines: ", str(G_retrieve_klines))
    print("G_retrieve_db_saved: ", str(G_retrieve_db_saved))
    print("G_retrieve_db_failed: ", str(G_retrieve_db_failed))
    print("-"*30)


def sync_day_kline_multi(stocks, startdate, enddate):
    # retrieve day kline & save to db
    partial_sync = partial(sync_day_kline, startdate=startdate, enddate=enddate)
    with futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as e:
        e.map(partial_sync, stocks)


def sync_day_kline(stock, startdate, enddate):
    try:
        try:
            print("Retrieve QFQ day kline for {} from {} to {}".format(stock, startdate, enddate))
            klines = retrieve_day_kline(stock, startdate, enddate)
            if klines is None:
                raise RuntimeError(
                    "ERROR: Cannot retrieve QFQ day kline for {} from {} to {}".format(stock, startdate, enddate))

            G_retrieve_stock_succeed.inc()
        except Exception:
            G_retrieve_stock_failed.inc()
            raise

        kline_len = len(klines)
        G_retrieve_klines.inc(kline_len)
        print("Store {} klines for {} from {} to {}".format(kline_len, stock, startdate, enddate))

        # get couchbase bucket
        bucket = Bucket(DBURL)

        partial_store = partial(store_day_kline, stock=stock, bucket=bucket)
        for date, kline in klines.iterrows():
            try:
                partial_store(date, kline)
                G_retrieve_db_saved.inc()
            except CouchbaseError:
                G_retrieve_db_failed.inc()

    except Exception as e:
        print('-' * 30)
        print("Exception:", e)
        # import traceback
        # traceback.print_exc()
        print('-' * 30)


def retrieve_day_kline(stock, startdate, enddate):
    return ts.get_h_data(stock, start=startdate, end=enddate, retry_count=TUSHARE_RETRIES)


def store_day_kline(date, kline, stock, bucket):
    date_str = date.to_pydatetime().strftime('%Y-%m-%d')
    key = KDAY_PREFIX + stock + '_' + date_str
    d = {
        'code': stock,
        'date': date_str,
        'type': 'stock_kday',
        'fqtype': 'qfq'
    }
    for k, v in kline.iteritems():
        d[k] = v

    rv = bucket.upsert(key, d)
    if not rv.success:
        raise RuntimeError("ERROR: upsert day kline for {} on {} failed!".format(stock, date_str))


def hist():
    stocks = []
    _do_sync_day_kline_multi(stocks, '2015-01-01', '2015-12-31')
    _do_sync_day_kline_multi(stocks, '2014-01-01', '2014-12-31')
    _do_sync_day_kline_multi(stocks, '2013-01-01', '2013-12-31')
    _do_sync_day_kline_multi(stocks, '2012-01-01', '2012-12-31')
    _do_sync_day_kline_multi(stocks, '2011-01-01', '2011-12-31')


def today():
    stocks = []
    from datetime import datetime
    today = datetime.today().strftime('%Y-%m-%d')
    _do_sync_day_kline_multi(stocks, today, today)


def main():
    stocks = []
    _do_sync_day_kline_multi(stocks, '2016-01-01', '2016-12-31')


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    from prometheus_client import start_http_server
    start_http_server(7777)

    main()
    # today()
    # cli()
