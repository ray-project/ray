---
myst:
  html_meta:
    description: "Start the Ray runtime on one machine with ray.init, from the CLI with ray start, or launch a multi-node cluster with ray up."
---

(start-ray)=

# Starting Ray

This page describes how to start Ray on a single machine or on a cluster of machines.

:::{tip}
{ref}`Install Ray <installation>` before you follow the instructions on this page.
:::

(what-is-the-ray-runtime)=

## What's the Ray runtime?

Ray programs parallelize and distribute work through an underlying *Ray runtime*. The Ray runtime consists of multiple services and processes that run in the background and handle communication, data transfer, scheduling, and more. You can start the Ray runtime on a laptop, a single server, or multiple servers.

You can start the Ray runtime in three ways:

* Implicitly through `ray.init()`. See {ref}`start-ray-init`.
* Explicitly through the CLI. See {ref}`start-ray-cli`.
* Explicitly through the cluster launcher. See {ref}`start-ray-up`.

In all cases, `ray.init()` tries to automatically find a Ray instance to connect to. It checks the following, in order:

1. The concrete address that you pass to `ray.init(address=<address>)`.
1. If you don't pass an address, or pass `"auto"`, the `RAY_ADDRESS` OS environment variable.
1. If `RAY_ADDRESS` isn't set, the latest Ray instance that `ray start` started on the same machine.

(start-ray-init)=

## Starting Ray on a single machine

Calling `ray.init()` starts a local Ray instance on your laptop or machine. This machine becomes the *head node*.

:::{note}
As of Ray 1.5, Ray calls `ray.init()` automatically the first time you use a Ray remote API.
:::

::::{tab-set}
:::{tab-item} Python
```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
# Other Ray APIs will not work until `ray.init()` is called.
ray.init()
```
:::

:::{tab-item} Java
```java
import io.ray.api.Ray;

public class MyRayApp {

  public static void main(String[] args) {
    // Other Ray APIs will not work until `Ray.init()` is called.
    Ray.init();
    ...
  }
}
```
:::

:::{tab-item} C++
```c++
#include <ray/api.h>
// Other Ray APIs will not work until `ray::Init()` is called.
ray::Init()
```
:::
::::

When the process that calls `ray.init()` exits, the Ray runtime also stops. To stop or restart Ray explicitly, use the shutdown API.

:::{note}
The behavior of `ray.shutdown()` depends on whether `ray.init()` started a new cluster or connected to an existing one:

* If `ray.init()` started a new local cluster, `ray.shutdown()` stops all the local Ray processes.
* If you connected to an existing cluster, for example with `ray.init(address="auto")` or `ray.init(address="ray://<ip>:<port>")`, `ray.shutdown()` only disconnects the client. It doesn't shut down the remote cluster.
:::

::::{tab-set}
:::{tab-item} Python
```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
import ray
ray.init()
... # ray program
ray.shutdown()
```
:::

:::{tab-item} Java
```java
import io.ray.api.Ray;

public class MyRayApp {

  public static void main(String[] args) {
    Ray.init();
    ... // ray program
    Ray.shutdown();
  }
}
```
:::

:::{tab-item} C++
```c++
#include <ray/api.h>
ray::Init()
... // ray program
ray::Shutdown()
```
:::
::::

To check whether Ray is initialized, use the `is_initialized` API.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray
ray.init()
assert ray.is_initialized()

ray.shutdown()
assert not ray.is_initialized()
```
:::

:::{tab-item} Java
```java
import io.ray.api.Ray;

public class MyRayApp {

public static void main(String[] args) {
        Ray.init();
        Assert.assertTrue(Ray.isInitialized());
        Ray.shutdown();
        Assert.assertFalse(Ray.isInitialized());
    }
}
```
:::

:::{tab-item} C++
```c++
#include <ray/api.h>

int main(int argc, char **argv) {
    ray::Init();
    assert(ray::IsInitialized());

    ray::Shutdown();
    assert(!ray::IsInitialized());
}
```
:::
::::

For the ways to configure Ray, see the {doc}`Configuration <configure>` documentation.

(start-ray-cli)=

(starting-ray-via-the-cli-ray-start)=

## Starting Ray through the CLI (`ray start`)

Run `ray start` from the CLI to start a single-node Ray runtime on a machine. This machine becomes the head node.

```bash
$ ray start --head --port=6379

Local node IP: 192.123.1.123
2020-09-20 10:38:54,193 INFO services.py:1166 -- View the Ray dashboard at http://localhost:8265

--------------------
Ray runtime started.
--------------------

...
```

To connect to this Ray instance, start a driver process on the same node where you ran `ray start`. `ray.init()` automatically connects to the latest Ray instance.

::::{tab-set}
:::{tab-item} Python
```{testcode}
import ray
ray.init()
```
:::

:::{tab-item} Java
```java
import io.ray.api.Ray;

public class MyRayApp {

  public static void main(String[] args) {
    Ray.init();
    ...
  }
}
```

```bash
java -classpath <classpath> \
  -Dray.address=<address> \
  <classname> <args>
```
:::

:::{tab-item} C++
```c++
#include <ray/api.h>

int main(int argc, char **argv) {
  ray::Init();
  ...
}
```

```bash
RAY_ADDRESS=<address> ./<binary> <args>
```
:::
::::

To create a Ray cluster, connect other nodes to the head node by calling `ray start` on those nodes as well. For more details, see {ref}`on-prem`. Calling `ray.init()` on any machine in the cluster connects to the same Ray cluster.

(start-ray-up)=

## Launching a Ray cluster (`ray up`)

You can launch Ray clusters with the {ref}`cluster launcher <cluster-index>`. The `ray up` command uses the Ray cluster launcher to start a cluster on the cloud, which creates a designated head node and worker nodes. `ray up` calls `ray start` to create the Ray cluster.

Your code needs to run on only one machine in the cluster, usually the head node.

To connect to the Ray cluster, call `ray.init` from one of the machines in the cluster. `ray.init` connects to the latest Ray cluster:

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
ray.init()
```

The machine that calls `ray up` isn't part of the Ray cluster, so calling `ray.init` on that machine doesn't attach to the cluster.

## What's next?

To deploy Ray in different settings, including {doc}`Kubernetes <../cluster/kubernetes/index>`, {doc}`YARN <../cluster/vms/user-guides/community/yarn>`, and {doc}`SLURM <../cluster/vms/user-guides/community/slurm>`, see the {doc}`deployment section <../cluster/getting-started>`.
