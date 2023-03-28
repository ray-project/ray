# __anti_pattern_start__
import ray
import time
import asyncio

@ray.remote
def f(x):
    time.sleep(1)
    return x

async def bad():
    o1 = f.remote(1)
    o2 = f.remote(2)
    o3 = f.remote(3)
    # These are blocking calls that block
    # the entire event loop.
    print(f"o1 is {ray.get(o1)}")
    print(f"o2 is {ray.get([o2])[0]}")
    ray.wait([o3])
    print("o3 is ready")

asyncio.run(bad())
# __anti_pattern_end__

# __better_approach_start__
async def good():
    o1 = f.remote(1)
    o2 = f.remote(2)
    o3 = f.remote(3)
    print(f"o1 is {await o1}")
    print(f"o2 is {(await asyncio.gather(o2))[0]}")
    await asyncio.wait([o3], return_when=asyncio.FIRST_COMPLETED)
    print("o3 is ready")

asyncio.run(good())
# __better_approach_end__
