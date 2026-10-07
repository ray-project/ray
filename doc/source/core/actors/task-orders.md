---
myst:
  html_meta:
    description: "How Ray orders actor method execution, and how that differs between synchronous single-threaded actors and async or threaded actors."
---

(actor-task-order)=

# Actor task execution order

## Synchronous, single-threaded actor
An actor receives tasks from multiple submitters, including the driver and workers. A synchronous, single-threaded actor executes tasks from the same submitter in submission order, unless you set `allow_out_of_order_execution` or Ray retries tasks. The actor doesn't start a task until all earlier tasks from the same submitter finish. If you set `max_task_retries` to a nonzero value for an actor, Ray doesn't guarantee task execution order when tasks retry.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray

@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def add(self, addition):
        self.value += addition
        return self.value

counter = Counter.remote()

# For tasks from the same submitter,
# they are executed according to submission order.
value0 = counter.add.remote(1)
value1 = counter.add.remote(2)

# Output: 1. The first submitted task is executed first.
print(ray.get(value0))
# Output: 3. The later submitted task is executed later.
print(ray.get(value1))
```

```{testoutput}
1
3
```
:::
::::


However, the actor doesn't guarantee execution order for tasks from different submitters. For example, if an unfulfilled argument blocks an earlier task, the actor can still execute tasks that a different worker submits.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import time
import ray

@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    def add(self, addition):
        self.value += addition
        return self.value

counter = Counter.remote()

# Submit task from a worker
@ray.remote
def submitter(value):
    return ray.get(counter.add.remote(value))

# Simulate delayed result resolution.
@ray.remote
def delayed_resolution(value):
    time.sleep(1)
    return value

# Submit tasks from different workers, with
# the first submitted task waiting for
# dependency resolution.
value0 = submitter.remote(delayed_resolution.remote(1))
value1 = submitter.remote(2)

# Output: 3. The first submitted task is executed later.
print(ray.get(value0))
# Output: 2. The later submitted task is executed first.
print(ray.get(value1))
```

```{testoutput}
3
2
```
:::
::::


## Asynchronous or threaded actor
{ref}`Asynchronous or threaded actors <async-actors>` don't guarantee task execution order. Ray might execute a task even while earlier tasks are still pending.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import time
import ray

@ray.remote
class AsyncCounter:
    def __init__(self):
        self.value = 0

    async def add(self, addition):
        self.value += addition
        return self.value

counter = AsyncCounter.remote()

# Simulate delayed result resolution.
@ray.remote
def delayed_resolution(value):
    time.sleep(1)
    return value

# Submit tasks from the driver, with
# the first submitted task waiting for
# dependency resolution.
value0 = counter.add.remote(delayed_resolution.remote(1))
value1 = counter.add.remote(2)

# Output: 3. The first submitted task is executed later.
print(ray.get(value0))
# Output: 2. The later submitted task is executed first.
print(ray.get(value1))
```

```{testoutput}
3
2
```
:::
::::
