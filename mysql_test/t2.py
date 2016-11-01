# coding=utf8

import asyncio
import aiomysql

from http_test.time import timethis

N = 1000

async def create_conn_pool(loop):
    return await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='root',
                                      db='test1', maxsize=600,
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

async def testall(loop):
    pool = await create_conn_pool(loop)
    await test_create_table(pool)
    await test_show_tables(pool)
    await test_insert(pool)
    await test_count(pool)
    await test_drop_table(pool)
    pool.close()
    await pool.wait_closed()


@timethis
def main():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(testall(loop))

if __name__ == '__main__':
    N = 50000
    main()
