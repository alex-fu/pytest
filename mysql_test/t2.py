# coding=utf8

import asyncio
import aiomysql

from http_test.time import timethis

N = 1000
Concurrency = 100

async def create_conn_pool(loop, db):
    return await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='root',
                                      db=db, maxsize=Concurrency,
                                      autocommit=True, loop=loop)

async def execute_sql(pool, sql, print_result=False):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            # await conn.commit()  # we have set autocommit
            if print_result:
                result = await cur.fetchall()
                print(result)

async def test_create_table(pool):
    sql = "create table t1(id INT, name VARCHAR(30), PRIMARY KEY(id));"
    await execute_sql(pool, sql)

async def test_show_tables(pool):
    sql = "show tables;"
    await execute_sql(pool, sql, print_result=True)

async def test_drop_table(pool):
    sql = "drop table t1;"
    await execute_sql(pool, sql)

async def insert(pool, x):
    sql = "insert into t1 values(%d, 'name%d')" % (x, x)
    await execute_sql(pool, sql, print_result=False)

async def test_insert(pool):
    await asyncio.wait([insert(pool, x) for x in range(1, N)])

async def test_count(pool):
    sql = "select count(1) from t1"
    await execute_sql(pool, sql, print_result=True)

async def select(pool, x):
    sql = "select * from t1 where id=%d" % x
    await execute_sql(pool, sql, print_result=False)

async def test_select(pool):
    await asyncio.wait([select(pool, x) for x in range(1, N)])

async def testall(loop, args):
    pool = await create_conn_pool(loop, args.database)
    stage = 0
    if args.action == 'prepare':
        stage = 1
    elif args.action == 'select':
        stage = 2
    elif args.action == 'cleanup':
        stage = 3
    else:
        print("Not support action: {}".format(args.action))
        exit(1)

    if stage == 1:
        await test_create_table(pool)
        await test_show_tables(pool)
        await test_insert(pool)
        await test_count(pool)
    elif stage == 2:
        await test_select(pool)
    elif stage == 3:
        await test_drop_table(pool)

    pool.close()
    await pool.wait_closed()


@timethis
def main():
    import argparse
    parser = argparse.ArgumentParser(description='Mysql test')
    parser.add_argument('-d', '--database', required=True, help='please give a database')
    parser.add_argument('-c', '--concurrency', default=100, type=int, help='please give concurrency connection number')
    parser.add_argument('-t', '--total-num', default=1000, type=int, help='please give totally insert/query number')
    parser.add_argument('action', choices=['prepare', 'select', 'cleanup'])
    args = parser.parse_args()
    print('Args: database: {}, concurrency: {}, total_num: {}'.format(args.database, args.concurrency, args.total_num))
    # print(vars(args))

    global N
    global Concurrency
    N = args.total_num
    Concurrency = args.concurrency

    loop = asyncio.get_event_loop()
    loop.run_until_complete(testall(loop, args))

if __name__ == '__main__':
    main()
