# flake8: noqa
import ray
import dask.array as da

z = da.ones(100)

# fmt: off
# __timer_callback_begin__
from ray.util.dask import RayDaskCallback, ray_dask_get
from timeit import default_timer as timer


class MyTimerCallback(RayDaskCallback):
    def _ray_pretask(self, key, object_refs):
        # Executed at the start of the Ray task.
        start_time = timer()
        return start_time

    def _ray_posttask(self, key, result, pre_state):
        # Executed at the end of the Ray task.
        execution_time = timer() - pre_state
        print(f"Execution time for task {key}: {execution_time}s")


with MyTimerCallback():
    # Any .compute() calls within this context will get MyTimerCallback()
    # as a Dask-Ray callback.
    z.compute(scheduler=ray_dask_get)
# __timer_callback_end__
# fmt: on

# fmt: off
# __ray_dask_callback_direct_begin__
def my_presubmit_cb(task, key, deps):
    print(f"About to submit task {key}!")

with RayDaskCallback(ray_presubmit=my_presubmit_cb):
    z.compute(scheduler=ray_dask_get)
# __ray_dask_callback_direct_end__
# fmt: on

# fmt: off
# __ray_dask_callback_subclass_begin__
class MyPresubmitCallback(RayDaskCallback):
    def _ray_presubmit(self, task, key, deps):
        print(f"About to submit task {key}!")

with MyPresubmitCallback():
    z.compute(scheduler=ray_dask_get)
# __ray_dask_callback_subclass_end__
# fmt: on

# fmt: off
# __multiple_callbacks_begin__
# The hooks for both MyTimerCallback and MyPresubmitCallback will be
# called.
with MyTimerCallback(), MyPresubmitCallback():
    z.compute(scheduler=ray_dask_get)
# __multiple_callbacks_end__
# fmt: on

# fmt: off
# __caching_actor_begin__
@ray.remote
class SimpleCacheActor:
    def __init__(self):
        self.cache = {}

    def get(self, key):
        # Raises KeyError if key isn't in cache.
        return self.cache[key]

    def put(self, key, value):
        self.cache[key] = value


class SimpleCacheCallback(RayDaskCallback):
    def __init__(self, cache_actor_handle, put_threshold=10):
        self.cache_actor = cache_actor_handle
        self.put_threshold = put_threshold

    def _ray_presubmit(self, task, key, deps):
        try:
            return ray.get(self.cache_actor.get.remote(str(key)))
        except KeyError:
            return None

    def _ray_pretask(self, key, object_refs):
        start_time = timer()
        return start_time

    def _ray_posttask(self, key, result, pre_state):
        execution_time = timer() - pre_state
        if execution_time > self.put_threshold:
            self.cache_actor.put.remote(str(key), result)


cache_actor = SimpleCacheActor.remote()
cache_callback = SimpleCacheCallback(cache_actor, put_threshold=2)
with cache_callback:
    z.compute(scheduler=ray_dask_get)
# __caching_actor_end__
# fmt: on
