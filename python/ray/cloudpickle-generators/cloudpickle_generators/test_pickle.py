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


async def g():
    # pass
    c = h.remote()
    r = await c
    print(f"Received r={r}")
    # print("g sleep 1")
    # await asyncio.sleep(1)
    # print("g sleep 2")
    # await asyncio.sleep(2)
    # print("g sleep 3")
    # await asyncio.sleep(3)

coro = g()
f1 = coro.send(None)
pprint(f1)
import pdb; pdb.set_trace()

f1.get_loop()._run_once()
pprint(f1)

# import pdb; pdb.set_trace()
new_coro = cloudpickle.loads(cloudpickle.dumps(coro))
new_coro.send(None)
# print(next(new_coro))
