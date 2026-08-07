---
myst:
  html_meta:
    description: "Practical tips for testing Ray programs, such as fixing the resource quantity with ray.init(num_cpus=...) to avoid flaky, parallelism-dependent tests. Read this when writing reliable tests for code that uses Ray."
---

# Tips for testing Ray programs

Ray programs can be tricky to test due to the nature of parallel programs. We've put together a list of tips and tricks for common testing practices for Ray programs.

```{contents}
:local:
```

## Tip 1: Fixing the resource quantity with `ray.init(num_cpus=...)`

By default, `ray.init()` detects the number of CPUs and GPUs on your local machine/cluster.

However, your testing environment may have significantly fewer resources. For example, a continuous integration (CI) environment often has far fewer cores available than your development machine.

If tests are written to depend on `ray.init()`, they may be implicitly written in a way that relies on a larger multi-core machine.

This may result in tests exhibiting unexpected, flaky, or faulty behavior that is hard to reproduce.

To overcome this, override the detected resources by setting them in `ray.init`, for example, `ray.init(num_cpus=2)`.

## Tip 2: Sharing the Ray cluster across tests if possible

It's safest to start a new Ray cluster for each test.

```{testcode}
import unittest

class RayTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=4, num_gpus=0)

    def tearDown(self):
        ray.shutdown()
```

However, starting and stopping a Ray cluster can incur a non-trivial amount of latency. For example, on a typical MacBook Pro laptop, starting and stopping can take nearly five seconds:

```bash
python -c 'import ray; ray.init(); ray.shutdown()'  3.93s user 1.23s system 116% cpu 4.420 total
```

Across 20 tests, this ends up being 90 seconds of added overhead.

Reusing a Ray cluster across tests can provide significant speedups to your test suite. This reduces the overhead to a constant, amortized quantity:

```{testcode}
class RayClassTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Start it once for the entire test suite/module
        ray.init(num_cpus=4, num_gpus=0)

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()
```

Depending on your application, there are certain cases where it may be unsafe to reuse a Ray cluster across tests. For example:

1. If your application depends on setting environment variables per process.
2. If your remote actor or task sets any sort of process-level global variables.

## Tip 3: Create a mini-cluster with `ray.cluster_utils.Cluster`

If writing an application for a cluster setting, you may want to mock a multi-node Ray cluster. You can do this with the `ray.cluster_utils.Cluster` utility.

:::{note}
On Windows, support for multi-node Ray clusters is experimental and untested. If you run into issues, file a report at <https://github.com/ray-project/ray/issues>.
:::

```{testcode}
from ray.cluster_utils import Cluster

# Starts a head-node for the cluster.
cluster = Cluster(
    initialize_head=True,
    head_node_args={
        "num_cpus": 10,
    })
```

After starting a cluster, you can execute a typical ray script in the same process:

```{testcode}
import ray

ray.init(address=cluster.address)

@ray.remote
def f(x):
    return x

for _ in range(1):
    ray.get([f.remote(1) for _ in range(1000)])

for _ in range(10):
    ray.get([f.remote(1) for _ in range(100)])

for _ in range(100):
    ray.get([f.remote(1) for _ in range(10)])

for _ in range(1000):
    ray.get([f.remote(1) for _ in range(1)])
```

You can also add multiple nodes, each with different resource quantities:

```{testcode}
mock_node = cluster.add_node(num_cpus=10)

assert ray.cluster_resources()["CPU"] == 20
```

You can also remove nodes, which is useful when testing failure-handling logic:

```{testcode}
cluster.remove_node(mock_node)

assert ray.cluster_resources()["CPU"] == 10
```

See [`cluster_utils.py`](https://github.com/ray-project/ray/blob/master/python/ray/cluster_utils.py) for more details.

## Tip 4: Be careful when running tests in parallel

Since Ray starts a variety of services, it's easy to trigger timeouts if too many services start at once. Therefore, when using tools such as [pytest xdist](https://pypi.org/project/pytest-xdist/) that run multiple tests in parallel, keep in mind that this may introduce flakiness into the test environment.
