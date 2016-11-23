# coding=utf8

from fn.iters import *
from fn import _

from strategy.testLine import TestLine
from strategy.utils import Bean

ST_NAME = 'transtrend'
ST_VERSION = 'v1'

def main():
    """
    Intention:
        A strategy to test whether the trans-trend strategy effective (test on day kline)

    Trade conditions:
        Open: 两天量价齐升
        Close: Open后2, 3, 4, 5, N天（可配置）的收益情况
        止损: 5% （可配置）

    Argument:
        1. Stock
        2. StartDate
        3. EndDate
        4. NDayClose
        5. StopPercent

    Result:
        'strategy_<strategyName>_<strategyVersion>_<startDate>_<endDate>_<stock>_<NDayClose>_<StopPercent>': {
            'trades': [
                {
                    'OpenDate': xxx,
                    'OpenPrice: xxx,
                    'CloseDate': xxx,
                    'ClosePrice: xxx
                },
                ...
            ]
        }

    Analyze:
        * 每次下注金额相同情况下，对每种参数配置，计算：
        1. 单个股票在回测时间段内的所有买卖交易（OpenDate, OpenPrice, CloseDate, ClosePrice）

        * 根据上述数据，展示以下内容：
        1. 单个股票随时间的收益率曲线
        2. 所有股票随时间的平均收益率曲线
        3. 指定股票组合随时间的平均收益率曲线

        4. 单个股票在不同配置下随时间的收益率对比曲线
        5. 所有股票在不同配置下随时间的平均收益率对比曲线
        6. 指定股票组合在不同配置下随时间的平均收益率对比曲线

        7. 所有股票随时间的收益率占比（带状图）
        8. 指定股票组合随时间的收益率占比（带状图）

        * 各种情况下的收益率曲线都可以添加与几个典型指数收益率的对比
    :return:
    """
    # parse argument
    import argparse
    parser = argparse.ArgumentParser(description='Trans-trend strategy')
    parser.add_argument('-s', '--start', required=True,
                        help='please set start date to do back test. Format: "2010-11-11"')
    parser.add_argument('-e', '--end', required=True,
                        help='please set end date. Format: same as --start')
    parser.add_argument('-n', '--ndayclose', required=True, type=int,
                        help='*Strategy specific argument*. How many days do you want to keep')
    parser.add_argument('-p', '--stop-percent', required=True, type=float,
                        help='*Strategy specific argument*. Stop percent(like 5%), means if you lose 5%, sell it')
    parser.add_argument('stock', nargs='*')
    args = parser.parse_args()

    # check arguments
    try:
        # check date format
        from strategy.utils import check_date
        check_date(args.start)
        check_date(args.end)
        if args.ndayclose < 1 or args.ndayclose > 200:
            raise RuntimeError('ndayclose should be in [1, 200]')
        if args.stop_percent <= 0 or args.stop_percent > 1:
            raise RuntimeError('stop percent should be in (0, 1]')
    except ValueError:
        print("ERROR: unrecognized date format! Should be like 2010-11-11")
        exit(1)

    print('-' * 30)
    print(
        'Arguments: \n- Test start on: {}\n- Test end on: {}\n- NDayClose: {}\n- Stop percent: {}\n- Stocks: {}'.format(
            args.start, args.end, args.ndayclose, args.stop_percent, args.stock
        ))
    print('-' * 30)

    if not args.stock:
        from strategy.dbutils import get_stocklist_fromdb
        args.stock = get_stocklist_fromdb()

    params = {'ndayclose': args.ndayclose, 'stop_percent': args.stop_percent}

    run(args.start, args.end, args.stock, params)


def run(startdate, enddate, stocks, params):
    from concurrent import futures
    from strategy.config import MAX_PROCESS_WORKERS
    partial_loop = partial(_loop, startdate=startdate, enddate=enddate, params=params)
    with futures.ThreadPoolExecutor(max_workers=1) as e:
        e.map(partial_loop, stocks)


def _loop(stock, startdate, enddate, params):
    testline = TestLine()
    testline.startdate = startdate
    testline.enddate = enddate
    testline.stock = stock

    params_bean = Bean()
    params_bean.ndayclose = params['ndayclose']
    params_bean.stop_percent = params['stop_percent']
    testline.params = params_bean
    loop(testline)


def loop(testLine):
    # get day klines between startdate and enddate for stock
    from strategy.dbutils import get_day_kline
    try:
        klines = get_day_kline(testLine.stock, testLine.startdate, testLine.enddate)
    except TimeoutError as e:
        print('Error when get day kline for {}! Detail: {}'.format(testLine.stock, e))

    # get rid of illegal kline
    (invalid_klines, valid_klines) = partition(_is_valid_kline, klines)
    for x in invalid_klines:
        print('Invalid KLine:', x)

    testLine.klines = list(valid_klines)
    # loop for each kline, check if it can open
    for index, _ in enumerate(testLine.klines):
        testLine.klineIndex = index
        handle(testLine)
    # complete loop
    complete(testLine)


def handle(testLine):
    yd_kline = testLine.ydKLine
    kline = testLine.getk()
    if kline is not None:
        testLine.ydKLine = kline

    # prepare flagList
    if yd_kline is None:
        testLine.flagList = [0]
        return
    else:
        testLine.flagList.append(_get_flag(yd_kline, kline))

    if len(testLine.flagList) <= 4:
        return

    position = testLine.position1()

    # should open?
    open = False
    if position.volume > 0:  # don't open if has position
        open = False
    elif testLine.flagList[-1] == 2 and testLine.flagList[-2] == 2 \
            and _lower_price(testLine.flagList[-3]) and _lower_price(testLine.flagList[-4]):
        open = True

    # if should open, use close price to open
    if open:
        testLine.open1(kline['close'])

    # should close?
    close = False
    if position.volume > 0:
        if testLine.klineIndex - position.klineIndex == testLine.params.ndayclose:
            close = True
        elif 1.0 - (float(kline['close']) / position.open_price) >= testLine.params.stop_percent:
            close = True

    if close:
        testLine.close1(kline['close'])


def complete(testLine):
    from strategy.config import ST_DBURL
    from couchbase.bucket import Bucket
    cb = Bucket(ST_DBURL)
    # store trade log to db
    # key format 'strategy_<strategyName>_<strategyVersion>_<startDate>_<endDate>_<stock>_<NDayClose>_<StopPercent>'
    key = 'strategy_' \
          + ST_NAME + '_' \
          + ST_VERSION + '_' \
          + testLine.startdate + '_' \
          + testLine.enddate + '_' \
          + testLine.stock + '_' \
          + str(testLine.params.ndayclose) + '_' \
          + str(testLine.params.stop_percent)
    trade_list = []
    for t in testLine.trades():
        trade_list.append(t.json())
        print(key, '=>', t.json())
    if trade_list:
        r = testLine.json()
        r['strategy_name'] = ST_NAME
        r['strategy_version'] = ST_VERSION
        r['ndayclose'] = testLine.params.ndayclose
        r['stop_percent'] = testLine.params.stop_percent
        r['trades'] = trade_list
        cb.upsert(key, r)


def _get_flag(ydKLine, kline):
    '''
    :return:
        2:  量价齐升
        1:  量跌价升
        -1: 量升价跌
        -2: 量价齐跌
        0:  特殊值
        (相等算跌)
    '''
    price_flag = kline['close'] > ydKLine['close']
    volume_flag = kline['volume'] > ydKLine['volume']
    if price_flag and volume_flag:
        return 2
    elif not price_flag and not volume_flag:
        return -2
    elif not price_flag and volume_flag:
        return -1
    else:
        return 1


def _lower_price(flag):
    return flag in [-1, -2]


def _higher_price(flag):
    return flag in [1, 2]

def _is_valid_kline(_):
    if _['close'] > 0 and _['volume'] > 0:
        return True
    else:
        return False


if __name__ == '__main__':
    main()
