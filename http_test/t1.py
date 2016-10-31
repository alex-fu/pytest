# coding=utf8

from urllib import request, parse


def execute_query(url, params):
    querystring = parse.urlencode(params)

    u = request.urlopen('http://localhost:9175'+url, querystring.encode('ascii'))
    resp = u.read()
    print(resp)


def execute_sql(params):
    url = '/api/v1/sql'
    execute_query(url, params)


def show_tables(db):
    params = {
        'format': 'json',
        'database': db,
        'query': 'show tables;'
    }
    return execute_sql(params)


def show_table_list(db):
    params = {
        # 'database': db,
        'order': 'desc'
    }
    url = '/api/v1/tables/list'
    return execute_query(url, params)


def insert_to_line(db):
    params = {
        'database': db,
        # 'table': 'line',
        'data': {
            'x': 15,
            'y': 15
        }
    }
    url = '/api/v1/tables/insert'
    return execute_query(url, params)

if __name__ == '__main__':
    db = 'test1'
    show_tables(db)
    show_table_list(db)   # return Unauthorized 401 error
    # insert_to_line(db)

