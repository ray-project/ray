---
myst:
  html_meta:
    description: "Assorted Ray Core topics: dynamic remote parameters, overloaded functions, inspecting cluster state, and OS tuning for large clusters."
---

# Miscellaneous topics

This page covers assorted Ray Core topics that range from dynamic remote parameters to operating system tuning for large clusters.

```{contents}
:local:
```

## Dynamic remote parameters

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

```{image} images/task_name_dashboard.png
:alt: Machine view of the Ray dashboard, listing the worker processes on one host with the task name of each.
```


## Overloaded functions
The Ray Java API supports calling overloaded Java functions remotely. Because of a limitation in Java compiler type inference, you must explicitly cast the method reference to the correct function type.

The following example calls an overloaded function as a normal task:

```java
public static class MyRayApp {

  public static int overloadFunction() {
    return 1;
  }

  public static int overloadFunction(int x) {
    return x;
  }
}

// Invoke overloaded functions.
Assert.assertEquals((int) Ray.task((RayFunc0<Integer>) MyRayApp::overloadFunction).remote().get(), 1);
Assert.assertEquals((int) Ray.task((RayFunc1<Integer, Integer>) MyRayApp::overloadFunction, 2).remote().get(), 2);
```

The following example calls overloaded methods of an actor:

```java
public static class Counter {
  protected int value = 0;

  public int increment() {
    this.value += 1;
    return this.value;
  }
}

public static class CounterOverloaded extends Counter {
  public int increment(int diff) {
    super.value += diff;
    return super.value;
  }

  public int increment(int diff1, int diff2) {
    super.value += diff1 + diff2;
    return super.value;
  }
}
```

```java
ActorHandle<CounterOverloaded> a = Ray.actor(CounterOverloaded::new).remote();
// Call an overloaded actor method by super class method reference.
Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 1);
// Call an overloaded actor method, cast method reference first.
a.task((RayFunc1<CounterOverloaded, Integer>) CounterOverloaded::increment).remote();
a.task((RayFunc2<CounterOverloaded, Integer, Integer>) CounterOverloaded::increment, 10).remote();
a.task((RayFunc3<CounterOverloaded, Integer, Integer, Integer>) CounterOverloaded::increment, 10, 10).remote();
Assert.assertEquals((int) a.task(Counter::increment).remote().get(), 33);
```

## Inspecting cluster state

Applications built on Ray often need information or diagnostics about the cluster. Common questions include the following:

1. How many nodes are in your autoscaling cluster?
1. What resources are available in your cluster, both used and total?
1. What objects are in your cluster?

To answer these questions, use the global state API.

### Node information

To get information about the current nodes in your cluster, use `ray.nodes()`:

```{eval-rst}
.. autofunction:: ray.nodes
   :noindex:
```

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
import ray

ray.init()
print(ray.nodes())
```

```{testoutput}
:options: +MOCK

  [{'NodeID': '2691a0c1aed6f45e262b2372baf58871734332d7',
    'Alive': True,
    'NodeManagerAddress': '192.168.1.82',
    'NodeManagerHostname': 'host-MBP.attlocal.net',
    'NodeManagerPort': 58472,
    'ObjectManagerPort': 52383,
    'ObjectStoreSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/plasma_store',
    'RayletSocketName': '/tmp/ray/session_2020-08-04_11-00-17_114725_17883/sockets/raylet',
    'MetricsExportPort': 64860,
    'alive': True,
    'Resources': {'CPU': 16.0, 'memory': 100.0, 'object_store_memory': 34.0, 'node:192.168.1.82': 1.0}}]
```

The preceding output includes the following fields:

- `NodeID`: A unique identifier for the raylet.
- `alive`: Whether the node is alive.
- `NodeManagerAddress`: The private IP address of the node that the raylet runs on.
- `Resources`: The total resource capacity on the node.
- `MetricsExportPort`: The port number that serves metrics through a {ref}`Prometheus endpoint <collect-metrics>`.

### Resource information

To get the current total resource capacity of your cluster, use `ray.cluster_resources()`.

```{eval-rst}
.. autofunction:: ray.cluster_resources
   :noindex:
```


To get the current available resource capacity of your cluster, use `ray.available_resources()`.

```{eval-rst}
.. autofunction:: ray.available_resources
   :noindex:
```

## Running large Ray clusters

The following tips help you run Ray on more than 1,000 nodes. At that scale, you might need to tune several system settings so that the machines can communicate with each other.

### Tuning operating system settings

All nodes and workers connect to the GCS, so the operating system (OS) has to support a large number of network connections.

#### Maximum open files

Every worker and raylet connects to the GCS, so configure the OS to support opening many TCP connections. On POSIX systems, check the current limit with `ulimit -n`. If the limit is small, increase it as your OS manual describes.

#### ARP cache

You also need to configure the Address Resolution Protocol (ARP) cache. In a large cluster, all the worker nodes connect to the head node, which adds many entries to the ARP table. Make sure the ARP cache is large enough to handle that many nodes. Otherwise, the head node hangs, and `dmesg` shows errors such as `neighbor table overflow message`.

On Ubuntu, tune the ARP cache size in `/etc/sysctl.conf` by increasing the values of `net.ipv4.neigh.default.gc_thresh1` through `net.ipv4.neigh.default.gc_thresh3`. For more details, see your OS manual.

### Benchmark

The benchmark uses the following machines:

- One head node: m5.4xlarge, with 16 vCPUs and 64 GB of memory
- 2,000 worker nodes: m5.large, with 2 vCPUs and 8 GB of memory

The benchmark uses the following OS settings:

- Set the maximum number of open files to 1048576.
- Increase the ARP cache size:
    - `net.ipv4.neigh.default.gc_thresh1=2048`
    - `net.ipv4.neigh.default.gc_thresh2=4096`
    - `net.ipv4.neigh.default.gc_thresh3=8192`


The benchmark uses the following Ray setting:

- `RAY_event_stats=false`

The test workload runs the following script:

- [`actor_test.py`](https://github.com/ray-project/ray/blob/master/release/benchmarks/distributed/many_nodes_tests/actor_test.py)



```{list-table} Benchmark result
:header-rows: 1

* - Number of actors
  - Actor launch time
  - Actor ready time
  - Total time
* - 20k, at 10 actors per node
  - 14.5s
  - 136.1s
  - 150.7s
```
