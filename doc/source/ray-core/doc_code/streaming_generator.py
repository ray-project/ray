# flake8: noqa

# fmt: off

# __streaming_generator_define_start__
import ray
import time

@ray.remote
def task():
    for i in range(5):
        time.sleep(5)
        yield i

# __streaming_generator_define_end__

# __streaming_generator_execute_start__
gen = task.remote()
# Blocks for 5 seconds.
ref = next(gen)
# return 0
ray.get(ref)
# Blocks for 5 seconds.
ref = next(gen)
# Return 1
ray.get(ref)

# Returns 2~4 every 5 seconds.
for ref in gen:
    print(ray.get(ref))

# __streaming_generator_execute_end__

# __streaming_generator_exception_start__
@ray.remote
def task():
    for i in range(5):
        time.sleep(1)
        if i == 1:
            raise ValueError
        yield i

gen = task.remote()
# it's okay.
ray.get(next(gen))

# Raises an exception
try:
    ray.get(next(gen))
except ValueError as e:
    print(f"Exception is raised when i == 1 as expected {e}")

# __streaming_generator_exception_end__

# __streaming_generator_actor_model_start__
@ray.remote
class Actor:
    def f(self):
        for i in range(5):
            yield i

@ray.remote
class AsyncActor:
    async def f(self):
        for i in range(5):
            yield i

@ray.remote(max_concurrency=5)
class ThreadedActor:
    def f(self):
        for i in range(5):
            yield i

actor = Actor.remote()
for ref in actor.f.remote():
    print(ray.get(ref))

actor = AsyncActor.remote()
for ref in actor.f.remote():
    print(ray.get(ref))

actor = ThreadedActor.remote()
for ref in actor.f.remote():
    print(ray.get(ref))

# __streaming_generator_actor_model_end__

# __streaming_generator_asyncio_start__
import asyncio

@ray.remote
def task():
    for i in range(5):
        time.sleep(1)
        yield i


async def main():
    async for ref in task.remote():
        print(await ref)

asyncio.run(main())

# __streaming_generator_asyncio_end__

# __streaming_generator_gc_start__
@ray.remote
def task():
    for i in range(5):
        time.sleep(1)
        yield i

gen = task.remote()
ref1 = next(gen)
del gen

# __streaming_generator_gc_end__

# __streaming_generator_concurrency_asyncio_start__
import asyncio

@ray.remote
def task():
    for i in range(5):
        time.sleep(1)
        yield i


async def async_task():
    async for ref in task.remote():
        print(await ref)

async def main():
    t1 = async_task()
    t2 = async_task()
    await asyncio.gather(t1, t2)

asyncio.run(main())
# __streaming_generator_concurrency_asyncio_end__

# __streaming_generator_wait_simple_start__
@ray.remote
def task():
    for i in range(5):
        time.sleep(5)
        yield i

gen = task.remote()

# Because it takes 5 seconds to make the first yield,
# with 0 timeout, the generator is unready.
ready, unready = ray.wait([gen], timeout=0)
print("timeout 0, nothing is ready.")
print(ready)
assert len(ready) == 0
assert len(unready) == 1

# Without a timeout argument, ray.wait waits until the given argument
# is ready. When a next item is ready, it returns.
ready, unready = ray.wait([gen])
print("Wait for 5 seconds. The next item is ready.")
assert len(ready) == 1
assert len(unready) == 0
next(gen)

# Because the second yield hasn't happened yet, 
ready, unready = ray.wait([gen], timeout=0)
print("Wait for 0 seconds. The next item is not ready.")
print(ready, unready)
assert len(ready) == 0
assert len(unready) == 1

# __streaming_generator_wait_simple_end__

# __streaming_generator_wait_complex_start__
from ray._raylet import ObjectRefGenerator

@ray.remote
def generator_task():
    for i in range(5):
        time.sleep(5)
        yield i

@ray.remote
def regular_task():
    for i in range(5):
        time.sleep(5)
    return

gen = [generator_task.remote()]
ref = [regular_task.remote()]
ready, unready = [], [*gen, *ref]
result = []

while unready:
    ready, unready = ray.wait(unready)
    for r in ready:
        if isinstance(r, ObjectRefGenerator):
            try:
                ref = next(r)
                result.append(ray.get(ref))
            except StopIteration:
                pass
            else:
                unready.append(r)
        else:
            result.append(ray.get(r))

# __streaming_generator_wait_complex_end__
