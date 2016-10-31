# coding=utf8
#import timeit

import requests

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


def execute_sql(sql, need_print_content=True):
    url = base_url() + '/api/v1/sql'
    command = {
        "format": "json",
        "query": sql,
        "database": get_database()
    }
    r = requests.post(url, data=command)
    if need_print_content:
        print(r.status_code)
        print(r.content)
    return r


def create_table_test():
    sql = "create table t1(id UINT64, name STRING, PRIMARY KEY(id));"
    execute_sql(sql)

    # test create table from .sql file
    with open("./create_md.sql", "r") as f:
        sql = f.read()
        execute_sql(sql)


def drop_table_test():
    pass


def list_table_test():
    sql = "show tables"
    r = execute_sql(sql)
    ret_dict = r.json()
    print(">>> TABLES <<<")
    for t in ret_dict['results'][0]['rows']:
        print(t[0])
    pass


def describe_table_test():
    sql = "describe t1"
    execute_sql(sql)
    pass


def sql_test():
    sql = "select * from zn1710_2016 order by id"
    r = execute_sql(sql, False)
    d = r.json()
    for row in d['results'][0]['rows']:
        print("%s %s %3s %s" % (row[1], row[21], row[22], row[5]))

@profile
def insert_test():
    from time import time
    start = time()
    for i in range(1, 10030):
        sql = "insert into t1 values(%d, 'name%d')" % (i, i)
        execute_sql(sql, False)
    end = time()
    print("Elapsed: %f sec" % (end - start))


if __name__ == '__main__':
    # create_table_test()
    # list_table_test()
    # describe_table_test()
    insert_test()
    # sql_test()
    # drop_table_test()
