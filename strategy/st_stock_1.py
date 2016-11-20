# coding=utf8


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
    parser.add_argument('stock', nargs='?')
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
    print('Arguments: \n- Test start on: {}\n- Test end on: {}\n- NDayClose: {}\n- Stop percent: {}\n- Stocks: '.format(
        args.start, args.end, args.ndayclose, args.stop_percent, args.stock
    ))
    print('-' * 30)

    run(args.start, args.end, args)


def run(startdate, enddate, args):
    stocks = args.stock
    # FIXME: change to use ThreadPool or ProcessPool
    for stock in stocks:
        loop(startdate, enddate, stock, args)


def loop(startdate, enddate, stock, args):
    # get day klines between startdate and enddate for stock
    from strategy.dbutils import get_day_kline
    klines = get_day_kline(stock, startdate, enddate)

    # get rid of illegal kline
    valid_klines = [k for k in klines if _is_valid_kline(k)]
    print(valid_klines)

    # loop for each kline, check if it can open


    # if it can open, calculate the close date and close price

    # store result to db

    pass


def _is_valid_kline(_):
    if _['close'] > 0 and _['volume'] > 0:
        return True
    else:
        return False


if __name__ == '__main__':
    main()
