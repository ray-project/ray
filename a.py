# import ray

# ray.init()

# @ray.remote
# class A:
#     def g(self):
#         while True:
#             yield "a" * 100000

# a = A.remote()
# while True:
#     gen = a.g.options(num_returns="streaming").remote()
#     import time
#     time.sleep(3)
#     ray.cancel(gen)


import asyncio

async def count():
    for i in range(10):
        print(i)
        await asyncio.sleep(1)

async def f():
    try:
        fut = asyncio.shield(count())
        print("start")
        await fut
    except asyncio.CancelledError:
        print("abc")
        import time
        time.sleep(5)
        print("done")


async def main():
    import threading
    e = asyncio.new_event_loop()
    t = threading.Thread(target=lambda: e.run_forever())
    t.daemon = True
    t.start()
    print("abc")
    fu = asyncio.run_coroutine_threadsafe(f(), e)
    await asyncio.sleep(2)
    fu.cancel()
    # fu.result()
    # await fu
    import time
    time.sleep(10)

asyncio.run(main())

