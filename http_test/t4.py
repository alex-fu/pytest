# coding=utf8
from concurrent import futures

import grequests
import requests

from http_test.debug import debug_entry
from http_test.time import timethis


def base_url():
    return 'http://localhost:9175'


def common_headers():
    return {'Authorization': 'Basic fuyf', 'X-EventQL-Namespace': 'trust'}


# API already changed now
# def create_table_test():
#     url = base_url() + '/api/v1/tables/create_table'
#     command = {
#         "table_name": "test_table",
#         "primary_key": ["time", "name"],
#         "database": "test1",
#         "columns": [
#             {
#                 "name": "time",
#                 "type": "DATETIME"
#             },
#             {
#                 "name": "name",
#                 "type": "STRING"
#             },
#             {
#                 "name": "value",
#                 "type": "DOUBLE"
#             }
#         ]
#     }
#     r = requests.post(url, json=command, headers=common_headers())
#     print(r.status_code)
#     print(r.content)

def get_database():
    return "test1"

def new_session():
    return requests.Session()

def close_session(s):
    s.close()

# global session
session = None
def execute_sql(sql, need_print_content=True):
    url = base_url() + '/api/v1/sql'
    command = {
        "format": "json",
        "query": sql,
        "database": get_database()
    }
    r = session.post(url, json=command)
    if need_print_content:
        print(r.status_code)
        print(r.content)
    return r


def grequest_execute_sql(sql):
    url = base_url() + '/api/v1/sql'
    command = {
        "format": "json",
        "query": sql,
        "database": get_database()
    }
    return grequests.post(url, json=command)

@debug_entry
def create_table_test():
    sql = "create table t1(id UINT64, name STRING, PRIMARY KEY(id));"
    execute_sql(sql)

@debug_entry
def drop_table_test():
    sql = "drop table t1"
    execute_sql(sql)

@debug_entry
def list_table_test():
    sql = "show tables"
    r = execute_sql(sql)
    ret_dict = r.json()
    print(">>> TABLES <<<")
    for t in ret_dict['results'][0]['rows']:
        print(t[0])
    pass

@debug_entry
def describe_table_test():
    sql = "describe t1"
    execute_sql(sql)
    pass

@debug_entry
def sql_test():
    sql = "select * from t1 order by id desc limit 1"
    r = execute_sql(sql, False)
    d = r.json()
    for row in d['results'][0]['rows']:
        print("%s %s" % (row[0], row[1]))


# @profile
@debug_entry
@timethis
def insert_test():
    for i in range(1, 10030):
        sql = "insert into t1 values(%d, 'name%d')" % (i, i)
        execute_sql(sql, False)


def insert(x):
    sql = "insert into t1 values(%d, 'm_name%d')" % (x, x)
    execute_sql(sql, False)
    return 1


def grequests_insert(x):
    sql = "insert into t1 values(%d, 'm_name%d')" % (x, x)
    return grequest_execute_sql(sql)


@debug_entry
@timethis
def multi_thread_insert_test():
    from multiprocessing.dummy import Pool
    p = Pool(4)
    p.map(insert, range(1, 10000))
    print("done")

@debug_entry
@timethis
def eventlet_insert_test():
    import eventlet
    p = eventlet.GreenPool()
    s = 0
    for r in p.imap(insert, range(1, 10000)):
        s += r
    print("done")


@debug_entry
@timethis
def gevent_insert_test():
    import gevent
    jobs = [gevent.spawn(insert, t) for t in range(1, 10000)]
    gevent.joinall(jobs)
    print("done")


@debug_entry
@timethis
def future_insert_test():
    with futures.ThreadPoolExecutor(max_workers=4) as e:
        fs = [e.submit(insert, x) for x in range(1, 10000)]
        futures.wait(fs)

@debug_entry
@timethis
def future_insert_test_2():
    with futures.ThreadPoolExecutor(max_workers=4) as e:
        e.map(insert, range(1, 10000))

@debug_entry
@timethis
def future_insert_test_3():
    with futures.ProcessPoolExecutor(max_workers=10) as e:
        e.map(insert, range(1, 10000))

@debug_entry
@timethis
def grequests_insert_test():
    rs = (grequests_insert(x) for x in range(1, 100000))
    rs2 = grequests.imap(rs, stream=True, size=1000)
    r = 0
    for _ in rs2:
        r += 1
    print(r)


@debug_entry
@timethis
def asyncio_insert_test():
    import asyncio
    async def inner_test():
        loop = asyncio.get_event_loop()
        for f in (loop.run_in_executor(None, insert, x) for x in range(1, 10000)):
            await f
    loop = asyncio.get_event_loop()
    loop.run_until_complete(inner_test())


if __name__ == '__main__':
    session = new_session()
    create_table_test()
    list_table_test()
    describe_table_test()
    # insert_test()
    # multi_thread_insert_test()
    # eventlet_insert_test()
    # gevent_insert_test()
    # future_insert_test()
    # future_insert_test_2()
    # future_insert_test_3()
    # grequests_insert_test()
    asyncio_insert_test()
    sql_test()
    drop_table_test()
    close_session(session)
