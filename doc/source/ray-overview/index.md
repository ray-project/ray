```{include} /_includes/overview/announcement.md
```

(gentle-intro)=

# Getting Started Guide

This tutorial will give you a quick tour of Ray's features.
To get started, we'll start by installing Ray.
Most of the examples in this guide are based on Python, but we'll also show you how to user Ray Core in Java.

````{panels}
:container: text-center
:column: col-lg-6 px-2 py-2
:card:

Python
^^^
To use Ray in Python, install it with
```
pip install ray
```

---

Java
^^^

To use Ray in Java, first add the [ray-api](https://mvnrepository.com/artifact/io.ray/ray-api) and
[ray-runtime](https://mvnrepository.com/artifact/io.ray/ray-runtime) dependencies in your project.

````

Want to build Ray from source or with docker? Need more details? 
Check out our detailed [installation guide](installation.rst).

## Ray AI Runtime Quick Start

Ray AI Runtime (AIR) is an open-source toolkit for building end-to-end ML applications.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> AI Runtime: Parallelizing Functions with Ray Tasks

TODO(rliaw): put together a quickstart
`````

## Ray Core Quick Start

Ray Core provides simple primitives for building and running distributed applications.
Below you find examples that show you how to turn your functions and classes easily into Ray tasks and actors,
for both Python and Java.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Core: Parallelizing Functions with Ray Tasks
:animate: fade-in-slide-down

````{tabbed} Python

First, you import Ray and and initialize it with `ray.init()`.
Then you decorate your function with ``@ray.remote`` to declare that you want to run this function remotely.
Lastly, you call that function with ``.remote()`` instead of calling it normally.
This remote call yields a future, a so-called Ray _object reference_, that you can then fetch with ``ray.get``.

```{code-block} python

import ray
ray.init()

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]

```
````

````{tabbed} Java

First, use `Ray.init` to initialize Ray runtime.
Then you can use `Ray.task(...).remote()` to convert any Java static method into a Ray task. 
The task will run asynchronously in a remote worker process. The `remote` method will return an ``ObjectRef``,
and you can then fetch the actual result with ``get``.

```{code-block} java

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;

public class RayDemo {

    public static int square(int x) {
        return x * x;
    }
    
    public static void main(String[] args) {
        // Intialize Ray runtime.
        Ray.init();
        List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
        // Invoke the `square` method 4 times remotely as Ray tasks.
        // The tasks will run in parallel in the background.
        for (int i = 0; i < 4; i++) {
            objectRefList.add(Ray.task(RayDemo::square, i).remote());
        }
        // Get the actual results of the tasks.
        System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
    }
}
````

In the above code block we defined some Ray Tasks. While these are great for stateless operations, sometimes you
must maintain the state of your application. You can do that with Ray Actors.

```{link-button} ../ray-core/walkthrough
:type: ref
:text: Learn more about Ray Core
:classes: btn-outline-primary btn-block
```

`````

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Core: Parallelizing Classes with Ray Actors
:animate: fade-in-slide-down

Ray provides actors to allow you to parallelize an instance of a class in Python or Java.
When you instantiate a class that is a Ray actor, Ray will start a remote instance
of that class in the cluster. This actor can then execute remote method calls and
maintain its own internal state.

````{tabbed} Python

```{code-block} python

import ray
ray.init() # Only call this once.

@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n

counters = [Counter.remote() for i in range(4)]
[c.increment.remote() for c in counters]
futures = [c.read.remote() for c in counters]
print(ray.get(futures)) # [1, 1, 1, 1]
```
````

````{tabbed} Java
```{code-block} java

import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RayDemo {

    public static class Counter {
    
        private int value = 0;
        
        public void increment() {
            this.value += 1;
        }
        
        public int read() {
            return this.value;
        }
    }
        
    public static void main(String[] args) {
        // Intialize Ray runtime.
        Ray.init();
        List<ActorHandle<Counter>> counters = new ArrayList<>();
        // Create 4 actors from the `Counter` class.
        // They will run in remote worker processes.
        for (int i = 0; i < 4; i++) {
            counters.add(Ray.actor(Counter::new).remote());
        }
        
        // Invoke the `increment` method on each actor.
        // This will send an actor task to each remote actor.
        for (ActorHandle<Counter> counter : counters) {
            counter.task(Counter::increment).remote();
        }
        // Invoke the `read` method on each actor, and print the results.
        List<ObjectRef<Integer>> objectRefList = counters.stream()
            .map(counter -> counter.task(Counter::read).remote())
            .collect(Collectors.toList());
        System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
    }
}

````
```{link-button} ../ray-core/walkthrough
:type: ref
:text: Learn more about Ray Core
:classes: btn-outline-primary btn-block
```

`````

## Ray Cluster Quick Start

You can deploy your applications on Ray clusters, often with minimal code changes to your existing code.
See an example of this below.

`````{dropdown} <img src="images/ray_svg_logo.svg" alt="ray" width="50px"> Clusters: Launching a Ray Cluster on AWS
:animate: fade-in-slide-down

Ray programs can run on a single machine, or seamlessly scale to large clusters.
Take this simple example that waits for individual nodes to join the cluster.

````{dropdown} example.py
:animate: fade-in-slide-down

```{literalinclude} ../../yarn/example.py
:language: python
```
````
You can also download this example from our [GitHub repository](https://github.com/ray-project/ray/blob/master/doc/yarn/example.py).
Go ahead and store it locally in a file called `example.py`.

To execute this script in the cloud, just download [this configuration file](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml),
or copy it here:

````{dropdown} cluster.yaml
:animate: fade-in-slide-down

```{literalinclude} ../../../python/ray/autoscaler/aws/example-full.yaml
:language: yaml
```
````

Assuming you have stored this configuration in a file called `cluster.yaml`, you can now launch an AWS cluster as follows:

```bash
ray submit cluster.yaml example.py --start
```

```{link-button} cluster-cloud
:type: ref
:text: Learn more about launching Ray Clusters
:classes: btn-outline-primary btn-block
```

`````


```{include} learn-more.md
```

```{include} /_includes/overview/announcement_bottom.md
```