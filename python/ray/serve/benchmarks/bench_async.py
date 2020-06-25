import asyncio
import ray
import time
import numpy as np


@ray.remote
class Bencher:
    async def run_bench(self, name, f, multi):
        for _ in range(2):
            await f()

        data = []
        for _ in range(5):
            start = time.perf_counter()
            await f()
            end = time.perf_counter()
            data.append(end - start)

        data = np.array(data) / multi * 1e6
        print(name, np.mean(data), np.std(data), "us")


ray.init()


@ray.remote
def echo(i):
    return i


oid = echo.remote(1)


async def sync():
    for _ in range(100):
        ray.get(oid)


async def async_get():
    for _ in range(100):
        await oid


bencher = Bencher.remote()
ray.get(bencher.run_bench.remote("sync", sync, 100))
ray.get(bencher.run_bench.remote("async promoted", async_get, 100))


async def main():
    @ray.remote
    def a():
        return 1

    oid = a.remote()

    data = []
    for _ in range(5):
        start = time.perf_counter()
        [ await oid for _ in range(500)]
        end = time.perf_counter()
        data.append((end - start) * 1e6 / 500)
    print("async not promoted", np.mean(data), np.std(data), "us")


asyncio.get_event_loop().run_until_complete(main())