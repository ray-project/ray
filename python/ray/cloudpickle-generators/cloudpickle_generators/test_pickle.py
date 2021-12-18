import asyncio
import ray
from ray import cloudpickle
from pprint import pprint


def f(a, b):
    for n in range(a):
        yield b + b[-1] * n


gen = f(4, 'ay')
print(next(gen))
print(next(gen))

new_gen = cloudpickle.loads(cloudpickle.dumps(gen))
print(next(new_gen))
print(next(new_gen))


@ray.remote
def h():
    return 1


ray.get(h.remote())


@ray.remote
def j():
    return 2


@ray.remote
def k():
    return 3


@ray.remote
class TestActor:
    async def test_method(self):
        return 4

async def g():
    sum = 0

    r1 = h.remote()
    n1 = await r1
    print(f"Received n1={n1}")
    await asyncio.sleep(1)
    sum += n1

    r2 = j.remote()
    n2 = await r2
    print(f"Received n2={n2}")
    await asyncio.sleep(1)
    sum += n2

    r3 = k.remote()
    n3 = await r3
    print(f"Received n3={n3}")
    await asyncio.sleep(1)
    sum += n3

    a = TestActor.remote()
    r4 = a.test_method.remote()
    n4 = await r4
    print(f"Received n4={n4}")
    await asyncio.sleep(1)
    sum += n4

    return sum

# import pdb; pdb.set_trace()

@ray.remote
class StepA:
    async def PreProcess(self):
        return 1


@ray.remote
class StepB:
    async def Compute(self, data):
        return data + 10


@ray.remote
class StepC:
    async def Store(self, data):
        return data + 100


a = StepA.remote()
b = StepB.remote()
c = StepC.remote()


async def flow():
    await asyncio.sleep(1)

    data = await a.PreProcess.remote()
    print("Finished StepA")
    await asyncio.sleep(1)

    value = await b.Compute.remote(data)
    print("Finished StepB")
    await asyncio.sleep(1)

    result = await c.Store.remote(value)
    print("Finished StepC")
    await asyncio.sleep(1)

    return result

print(f"local result={asyncio.run(flow())}")

print(f"coroutine result={asyncio.run(ray.util.execute(flow()))}")


# coro = g()
# result = None

# while True:
#     # import pdb; pdb.set_trace()
#     try:
#         fut = coro.send(None)
#     except StopIteration as val:
#         result = val.value
#         break
#
#     if hasattr(fut, "_object_ref"):
#         print(f"ObjectRef fut={fut}")
#         new_coro = cloudpickle.loads(cloudpickle.dumps(coro))
#         coro = new_coro
#     else:
#         print(f"Not ObjectRef fut={fut}")
#
#     # TODO: figure out a way to yield thread?
#     fut.get_loop().run_until_complete(fut)


# print(f"result={result}")
