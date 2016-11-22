
HOST = 'localhost'

BUCKET_NAME = 'stock'
DBURL = 'couchbase://' + HOST + '/' + BUCKET_NAME
KDAY_PREFIX = 'kday_'
STOCK_PREFIX = 'stock_'

STRATEGY_BUCKET_NAME = 'strategy'
ST_DBURL = 'couchbase://' + HOST + '/' + STRATEGY_BUCKET_NAME

MAX_WORKERS = 8
TUSHARE_RETRIES = 10
