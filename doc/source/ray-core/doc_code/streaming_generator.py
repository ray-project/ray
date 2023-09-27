# __program_start__
import ray
# This API is subject to change.
from ray._raylet import StreamingObjectRefGenerator

# fmt: off
# __streaming_generator_define_start__
import time


@ray.remote(num_returns="streaming")
def task():
    for i in range(5):
        time.sleep(5)
        yield i

# __streaming_generator_define_end__

# __streaming_generator_execute_start__

gen = task.remote()
# Will block for 5 seconds.
ref = next(gen)
# return 0
ray.get(ref)
# Return 1~4 every 5 seconds.
for ref in gen:
    print(ray.get(ref))

# __streaming_generator_execute_end__

# __streaming_generator_exception_start__

@ray.remote(num_returns="streaming")
def task():
    for i in range(5):
        time.sleep(1)
        if i == 1:
            raise ValueError
        yield i

gen = task.remote()
# it is okay.
ray.get(next(gen))

# it raises an exception
try:
    ray.get(next(gen))
except ValueError as e:
    print(f"Exception is raised when i == 1 as expected {e}")

# __streaming_generator_exception_end__

# __streaming_generator_asyncio_start__
import asyncio

@ray.remote(num_returns="streaming")
def task():
    for i in range(5):
        time.sleep(1)
        yield i


async def main():
    async for ref in task.remote():
        print(await ref)

asyncio.run(main())

# __streaming_generator_asyncio_end__