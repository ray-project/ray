---
myst:
  html_meta:
    description: "Change the resource requirements, number of returns, or name of a Ray task or actor at submission time with .options()."
---

(core-dynamic-options)=

# Set remote parameters dynamically

To adjust the resource requirements or return values of `ray.remote` dynamically during execution, use `.options`.

For example, the following code instantiates multiple copies of the same actor with varying resource requirements. To create these actors successfully, start Ray with sufficient CPU resources and the relevant custom resources:

```{testcode}
import ray

@ray.remote(num_cpus=4)
class Counter(object):
    def __init__(self):
        self.value = 0

    def increment(self):
        self.value += 1
        return self.value

a1 = Counter.options(num_cpus=1, resources={"Custom1": 1}).remote()
a2 = Counter.options(num_cpus=2, resources={"Custom2": 1}).remote()
a3 = Counter.options(num_cpus=3, resources={"Custom3": 1}).remote()
```

You can specify different resource requirements for tasks, but not for actor methods:

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
ray.init(num_cpus=1, num_gpus=1)

@ray.remote
def g():
    return ray.get_gpu_ids()

object_gpu_ids = g.remote()
assert ray.get(object_gpu_ids) == []

dynamic_object_gpu_ids = g.options(num_cpus=1, num_gpus=1).remote()
assert ray.get(dynamic_object_gpu_ids) == [0]
```

You can also vary the number of return values for tasks and actor methods:

```{testcode}
@ray.remote
def f(n):
    return list(range(n))

id1, id2 = f.options(num_returns=2).remote(2)
assert ray.get(id1) == 0
assert ray.get(id2) == 1
```

You can also specify a name for tasks and actor methods at task submission time:

```{testcode}
import psutil

@ray.remote
def f(x):
   assert psutil.Process().cmdline()[0] == "ray::special_f"
   return x + 1

obj = f.options(name="special_f").remote(3)
assert ray.get(obj) == 4
```

This name appears as the task name in the machine view of the dashboard and in the logs. For a Python task, it also appears as the worker process name while the task runs.

```{image} ../images/task_name_dashboard.png
:alt: Machine view of the Ray dashboard, listing the worker processes on one host with the task name of each.
```
