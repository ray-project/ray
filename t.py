import asyncio
import time

import ray

ray.init()


@ray.remote
class Actor:
    def f(self, ref):
        for _ in range(3):
            yield ref

    async def async_f(self, ref):
        for _ in range(3):
            yield ref

    def g(self):
        return 3


a = Actor.remote()


def generator_main():
    b = ray.put(3)
    generator = a.f.remote(b)
    del b
    print(generator)
    print(next(generator))
    print(next(generator))
    print(next(generator))

    generator = a.f.remote(ray.put(3))
    for i in generator:
        print(i)


def async_task_generator_main():
    b = ray.put(3)
    generator = a.async_f.remote(b)
    del b
    print(generator)
    print(next(generator))
    print(next(generator))
    print(next(generator))

    generator = a.f.remote(ray.put(3))
    for i in generator:
        print(i)


# async def async_generator():
#     for i in range(3):
#         await asyncio.sleep(1)
#         yield i


async def main():
    async_generator = a.f.remote(ray.put(3))
    print(async_generator)
    async for value in async_generator:
        print("Received:", value)


async def async_task_main():
    async_generator = a.async_f.remote(ray.put(3))
    print(async_generator)
    async for value in async_generator:
        print("Received:", value)


def generator_gc():
    import numpy as np

    arr = np.random.rand(5 * 1024 * 1024)  # 40 MB
    b = ray.put(arr)
    g = a.f.remote(b)
    print(b.hex())
    print("wait a bit")
    time.sleep(10)
    del b
    print(next(g))
    print("wait a bit")
    time.sleep(10)
    print("GC")
    del g
    print("Is it GC'ed? check ray list objects")
    time.sleep(300)


asyncio.run(main())
generator_main()
async_task_generator_main()
asyncio.run(async_task_main())
generator_gc()

# Test lineage reconstruction?
# Test GC
