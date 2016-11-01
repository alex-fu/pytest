# coding=utf8

import os
import sys

from requests.adapters import HTTPAdapter

use_gevent = (not sys.flags.ignore_environment
          and bool(os.getenv('USE_GEVENT')))
print("USE_GEVENT? {}".format(use_gevent))
use_grequests = (not sys.flags.ignore_environment
          and bool(os.getenv('USE_GREQUEST')))
print("USE_GREQUEST? {}".format(use_grequests))

if use_gevent:
    from gevent.monkey import patch_all
    patch_all()
    print("Patch all for gevent")
    import gevent

if use_grequests:
    import grequests


from concurrent import futures
import requests

from http_test.debug import debug_entry
from http_test.time import timethis

N = 100000

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
    return grequests.post(url, json=command, session=session)

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
    sql = "select count(*) from t1"
    r = execute_sql(sql, False)
    d = r.json()
    for row in d['results'][0]['rows']:
        print("%s" % (row[0]))


# @profile
@debug_entry
@timethis
def insert_test():
    for i in range(1, N):
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
    p.map(insert, range(1, N))
    print("done")


@debug_entry
@timethis
def multi_process_insert_test():
    from multiprocessing import Pool
    p = Pool(4)
    p.map(insert, range(1, N))
    print("done")

@debug_entry
@timethis
def eventlet_insert_test():
    import eventlet
    eventlet.monkey_patch()
    print(eventlet.patcher.is_monkey_patched('socket'))
    p = eventlet.GreenPool()
    # s = 0
    # for r in p.imap(insert, range(1, N)):
    #     s += r
    print(sum(p.imap(insert, range(1, N))))
    print("done")


@debug_entry
@timethis
def gevent_insert_test():
    from gevent.pool import Pool
    pool = Pool(100)
    pool.map(insert, range(1, N))
    print("done")


@debug_entry
@timethis
def future_insert_test():
    with futures.ThreadPoolExecutor(max_workers=4) as e:
        fs = [e.submit(insert, x) for x in range(1, N)]
        futures.wait(fs)

@debug_entry
@timethis
def future_insert_test_2():
    with futures.ThreadPoolExecutor(max_workers=4) as e:
        e.map(insert, range(1, N))

@debug_entry
@timethis
def future_insert_test_3():
    with futures.ProcessPoolExecutor(max_workers=10) as e:
        e.map(insert, range(1, N))

@debug_entry
@timethis
def grequests_insert_test():
    rs = (grequests_insert(x) for x in range(1, N))
    rs2 = grequests.imap(rs, stream=True, size=1000)
    r = 0
    for _ in rs2:
        r += 1
    print(r)


@debug_entry
@timethis
def asyncio_insert_test():
    import asyncio
    import aiohttp
    import json
    async def execute(s, sql, need_print_content=True):
        url = base_url() + '/api/v1/sql'
        headers = {'content-type': 'application/json'}
        command = {
            "format": "json",
            "query": sql,
            "database": get_database()
        }
        r = await s.post(url, data=json.dumps(command), headers=headers)
        r.close()
        # async with s.post(url, data=json.dumps(command), headers=headers) as resp:
        #     await resp.json()
        #     if need_print_content:
        #         print(await resp.json())

    async def insert(s, x):
        sql = "insert into t1 values(%d, 'm_name%d')" % (x, x)
        # if x % 1000 == 0:
        #     print(x)
        await execute(s, sql, False)

    async def insert_test(s):
        await asyncio.wait([insert(s, x) for x in range(1, N)])

    loop = asyncio.get_event_loop()
    conn = aiohttp.TCPConnector(limit=100, loop=loop)
    with aiohttp.ClientSession(connector=conn, loop=loop) as sess:
        loop.run_until_complete(insert_test(sess))


if __name__ == '__main__':
    N = 10000
    session = new_session()
    session.mount('http://', HTTPAdapter(max_retries=5))
    create_table_test()
    # list_table_test()
    # describe_table_test()
    # insert_test()
    # multi_thread_insert_test()
    # multi_process_insert_test()
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
