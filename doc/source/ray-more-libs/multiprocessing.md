---
myst:
  html_meta:
    description: "Scale multiprocessing.Pool programs to a Ray cluster with ray.util.multiprocessing.Pool as a drop-in replacement."
---

(ray-multiprocessing)=

# Distributed multiprocessing.Pool


Ray supports running distributed Python programs with the [multiprocessing.Pool API](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool) using {doc}`Ray Actors </ray-core/actors>` instead of local processes. This makes it easy to scale existing applications that use `multiprocessing.Pool` from a single node to a cluster.


## Quickstart

To get started, first {doc}`install Ray </ray-overview/installation>`, then use `ray.util.multiprocessing.Pool` in place of `multiprocessing.Pool`. This will start a local Ray cluster the first time you create a `Pool` and distribute your tasks across it. See the [Run on a Cluster](#run-on-a-cluster) section below for instructions to run on a multi-node Ray cluster instead.

```python
from ray.util.multiprocessing import Pool

def f(index):
    return index

pool = Pool()
for result in pool.map(f, range(100)):
    print(result)
```

The full `multiprocessing.Pool` API is currently supported. Please see the [multiprocessing documentation](https://docs.python.org/3/library/multiprocessing.html#module-multiprocessing.pool) for details.

:::{warning}
The `context` argument in the `Pool` constructor is ignored when using Ray.
:::


## Run on a Cluster

This section assumes that you have a running Ray cluster. To start a Ray cluster, see the {ref}`cluster setup <cluster-index>` instructions.

To connect a `Pool` to a running Ray cluster, you can specify the address of the head node in one of two ways:

- By setting the `RAY_ADDRESS` environment variable.
- By passing the `ray_address` keyword argument to the `Pool` constructor.

```python
from ray.util.multiprocessing import Pool

# Starts a new local Ray cluster.
pool = Pool()

# Connects to a running Ray cluster, with the current node as the head node.
# Alternatively, set the environment variable RAY_ADDRESS="auto".
pool = Pool(ray_address="auto")

# Connects to a running Ray cluster, with a remote node as the head node.
# Alternatively, set the environment variable RAY_ADDRESS="<ip_address>:<port>".
pool = Pool(ray_address="<ip_address>:<port>")
```

You can also start Ray manually by calling `ray.init()` (with any of its supported configuration options) before creating a `Pool`.
