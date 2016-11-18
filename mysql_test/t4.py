from concurrent import futures

import MySQLdb

from http_test.debug import debug_entry
from mysql_test.PooledDB import PooledDB

from http_test.time import timethis


def connect_mysql(host, port, user, password, db):
    return MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db, autocommit=True)

def select(conn, x):
    cursor = conn.cursor()
    cursor.execute("select * from t1 where id={};".format(x))
    # print("rowcount: {}".format(cursor.rowcount))
    # print(cursor.fetchone())
    cursor.close()

db = None
def select_test(x):
    conn = connect_mysql('127.0.0.1', 3306, "root", "root", db)
    # print(conn._con._con.get_host_info())
    # print(conn._con._con.get_server_info())
    select(conn, x)


@timethis
def main():
    import argparse
    parser = argparse.ArgumentParser(description='mysql select benchmark(sync)')
    parser.add_argument('-d', '--database', required=True)
    parser.add_argument('-c', '--concurrency', default=100, type=int)
    parser.add_argument('-t', '--total-num', default=1000, type=int)
    args = parser.parse_args()
    print('Args: database: {}, concurrency: {}, total_num: {}'.format(args.database, args.concurrency, args.total_num))

    global db
    db = args.database

    with futures.ProcessPoolExecutor(max_workers=args.concurrency) as e:
        e.map(select_test, range(1, args.total_num))

if __name__ == '__main__':
    main()
