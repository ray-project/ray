import ray
import time


# By adding the `@ray.remote` decorator, a regular Python function
# becomes a Ray remote function.
@ray.remote
def my_function():
    return 1


# To invoke this remote function, use the `remote` method.
# This will immediately return an object ref (a future) and then create
# a task that will be executed on a worker process.
obj_ref = my_function.remote()

# The result can be retrieved with ``ray.get``.
assert ray.get(obj_ref) == 1


@ray.remote
def slow_function():
    time.sleep(10)
    return 1


# Invocations of Ray remote functions happen in parallel.
# All computation is performed in the background, driven by Ray's internal event loop.
results = []
for _ in range(4):
    # this doesn't block
    results.append(slow_function.remote())

ray.get(results)
