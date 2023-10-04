import time
import concurrent
import asyncio
from concurrent.futures import ThreadPoolExecutor
tp = ThreadPoolExecutor(max_workers=1)

def count():
    print("count")
    for i in range(5):
        print(i)
        time.sleep(1)

async def nothing():
    print("nothing")
    for i in range(3):
        print(i)
        time.sleep(1)

async def f():
    loop = asyncio.get_running_loop()
    # fut = loop.run_in_executor(tp, count())
    try:
        print("start")
        await nothing()
    except asyncio.CancelledError:
        print("nothing canceled")
    try:
        await loop.run_in_executor(tp, count)
    except asyncio.CancelledError:
        print("threadpool canceled")


async def main():
    import threading
    ev = threading.Event()
    e = asyncio.new_event_loop()
    t = threading.Thread(target=lambda: e.run_forever())
    ae = asyncio.Event(loop = e)
    t.daemon = True
    t.start()
    async def g():
        try:
            return await f()
        finally:
            print("f finished")
            ev.set()
            ae.set()

    fu = asyncio.run_coroutine_threadsafe(g(), e)
    await asyncio.sleep(2)
    # print(fu.cancel())
    print("cancelled")
    try:
        # fu.result()
        async def wait():
            await ae.wait()
        asyncio.run_coroutine_threadsafe(wait(), e).result()
    except concurrent.futures.CancelledError:
        print("Exception raised")
        s = time.time()
        ev.wait()
        print(f"took {time.time() - s}")
    print("done")
    time.sleep(10)

asyncio.run(main())
