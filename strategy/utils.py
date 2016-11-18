# coding=utf8


def check_date(date):
    from datetime import datetime
    datetime.strptime(date, '%Y-%m-%d')
