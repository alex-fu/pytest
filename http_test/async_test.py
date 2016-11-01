import aiohttp
import asyncio
import time

async def do_http_get(session):
    r = await session.get("http://localhost:9175/api/v1/sql")
    r.close()
    # async with session.get("http://localhost:9175/api/v1/sql") as resp:
    #     await resp.read()


async def do_sleep(l):
    await asyncio.sleep(l)


async def slow_operation(session, n):
    # await asyncio.sleep(5)
    await do_sleep(5)
    await do_http_get(session)
    # print("Slow operation {} complete".format(n))


async def main(session):
    start = time.time()
    await asyncio.wait([slow_operation(session, x) for x in range(1, 10000)])
    end = time.time()
    print('Complete in {} second(s)'.format(end-start))


loop = asyncio.get_event_loop()
with aiohttp.ClientSession(loop=loop) as session:
    loop.run_until_complete(main(session))
