import asyncio
import aiomysql


async def test_example(loop):
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='root',
                                      db='test1', maxsize=200, loop=loop)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            sql = "create table t1(id INT, name VARCHAR(30), PRIMARY KEY(id));"
            await cur.execute(sql)
            # await conn.commit()
            # print(cur.description)
    pool.close()
    await pool.wait_closed()


loop = asyncio.get_event_loop()
loop.run_until_complete(test_example(loop))
