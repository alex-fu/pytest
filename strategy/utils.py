# coding=utf8


def check_date(date):
    from datetime import datetime
    datetime.strptime(date, '%Y-%m-%d')


class Bean(object):
    def __getattr__(self, item):
        try:
            return super(Bean, self).__getattribute__(item)
        except:
            return None

    def __setattr__(self, key, value):
        super(Bean, self).__setattr__(key, value)


if __name__ == '__main__':
    b = Bean()
    b.attr = 'ccc'
    print(b.attr)
    print(b.attr1)
