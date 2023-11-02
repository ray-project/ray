import time
import asyncio
import ray
ray.init()

@ray.remote
def f():
    return "hi" * 100

@ray.remote(num_returns="streaming")
def g():
    yield "hi" * 100

@ray.remote
class A:
    async def f(self):
        ref = f.remote()
        ray.get(ref)

        s = time.time()
        ray.wait([ref], timeout=0)
        print(f"sync wait takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        ray.get([ref], timeout=0)
        print(f"sync get takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        await asyncio.wait([ref])
        print(f"wait takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        await ref
        print(f"get takes {(time.time() - s) * 1000 * 1000 } us")

    async def g(self):
        gen1 = g.remote()
        gen2 = g.remote()
        ray.get(gen1._generator_ref)
        ray.get(gen2._generator_ref)

        s = time.time()
        ref = next(gen1)
        print(f"sync next takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        ray.get([ref], timeout=0)
        print(f"sync get takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        ref2 = await gen2.__anext__()
        print(f"anext takes {(time.time() - s) * 1000 * 1000 } us")

        s = time.time()
        await ref2
        print(f"get takes {(time.time() - s) * 1000 * 1000 } us")


a = A.remote()
ray.get([a.g.remote() for _ in range(1)])
