import asyncio
import time

async def slow_operation(n):
    await asyncio.sleep(1)
    print("Slow operation {} complete".format(n))


async def main():
    start = time.time()
    await asyncio.wait([
        slow_operation(1),
        slow_operation(2),
        slow_operation(3),
    ])
    end = time.time()
    print('Complete in {} second(s)'.format(end-start))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
