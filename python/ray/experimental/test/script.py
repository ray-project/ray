import asyncio
import time
import logging
import pytest

import ray
from ray.experimental import async_api
ray.init()
async_api.init()


@ray.remote
def f(n):
    time.sleep(n)
    return n


x = f.remote(0.5)
xf = x.as_future()
print(xf.inner_future)
# asyncio.get_event_loop().run_until_complete(xf)
print(xf.inner_future)


@ray.remote
async def waiter(f):
    xf = f.as_future()
    await xf


z = waiter.remote(x)