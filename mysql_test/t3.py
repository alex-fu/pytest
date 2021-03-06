from concurrent import futures

import MySQLdb

from http_test.debug import debug_entry
from mysql_test.PooledDB import PooledDB

from http_test.time import timethis


def connect_mysql_pool(pool_size, host, port, user, password, db):
    return PooledDB(MySQLdb, maxcached=pool_size, maxconnections=pool_size, blocking=True,
                    host=host, port=port, user=user, passwd=password, db=db, autocommit=True)

def select(conn, x):
    cursor = conn.cursor()
    cursor.execute("select * from t1 where id={};".format(x))
    # print("rowcount: {}".format(cursor.rowcount))
    # print(cursor.fetchone())
    cursor.close()


pool = None


def select_test(x):
    conn = pool.connection()
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

    global pool
    pool = connect_mysql_pool(args.concurrency, '127.0.0.1', 3306, "root", "root", args.database)
    with futures.ThreadPoolExecutor(max_workers=400) as e:
        e.map(select_test, range(1, args.total_num))

if __name__ == '__main__':
    main()
