---
layout: post
title: "Implementing A Parameter Server in 15 Lines of Python with Ray"
excerpt: "This post describes how to implement a parameter server in Ray."
date: 2018-07-13 14:00:00
---

A parameter server (like a database) is normally built and shipped as a
standalone system. This post describes how to use [Ray][1] to implement a
parameter server in a few lines of code. This is powerful for two reasons:

1. It is orders of magnitude simpler to deploy applications that use parameter
servers.
2. The behavior of the parameter server is much more configurable and flexible.

*[Ray][1] is a general-purpose framework for parallel and distributed Python.
Ray also includes high-performance libraries targeting AI applications, for
example [hyperparameter tuning][5] and [reinforcement learning][4].*

## What is a Parameter Server?

A parameter server is a key-value store. The **values** are the parameters of a
machine-learning model (e.g., a neural network). The **keys** index the model
parameters.

For example, in a movie **recommendation system**, there may be one key per user
and one key per movie. For each user and movie, there are corresponding
user-specific and movie-specific parameters. In a **language-modeling**
application, there may be one key per word, and one vector of parameters per
word. In its simplest form, a parameter server may implicitly have a single key,
and allow all of the parameters to be retrieved and updated at once.

In its simplest form, a parameter server can be implemented as a Ray actor (15
lines) as follows.

```python
import numpy as np


@ray.remote
class ParameterServer(object):
    def __init__(self, dim):
        # Alternatively, params could be a dictionary mapping keys to numpy
        # arrays.
        self.params = np.zeros(dim)

    def get_params(self):
        return self.params

    def update_params(self, grad):
        self.params += grad
```

Here we assume that the update is a gradient which should be added to the
parameter vector. However, different choices could be made.

**A parameter server typically exists as a remote process or service**, and
interacts with clients through remote procedure calls. To instantiate the
parameter server as a remote actor, we can do the following.

```python
# Create a parameter server process.
ps = ParameterServer.remote(10)
```

Now, suppose we want to start some worker tasks that continuously compute
gradients and update the model parameters. Each worker will run in a loop that
does three things:
1. Get the latest parameters.
2. Compute an update to the parameters.
3. Update the parameters.

As a Ray remote function (though the worker could also be an actor), this looks
like the following.

```python
@ray.remote
def worker(ps):
    for _ in range(100):
        # Get the latest parameters.
        params = ray.get(ps.get_params.remote())

        # Compute a gradient update. Here we just make a fake update, but in
        # practice this would use a library like TensorFlow and would also take
        # in a batch of data.
        grad = np.ones(10)
        time.sleep(0.2)

        # Update the parameters.
        ps.update_params.remote(grad)
```

Then we can start several worker tasks as follows.

```python
# Start 2 workers.
for _ in range(2):
    worker.remote(ps)
```

## Additional Extensions

Here we describe some important modifications to the above design. We describe
additional natural extensions for dealing with stragglers in [this paper][3].

**Sharding Across Multiple Parameter Servers:** When your parameters are large and your cluster is large, a single parameter
server may not suffice because the application could be bottlenecked by the
network bandwidth into and out of the machine that the parameter server is on
(especially if there are many workers).

A natural solution in this case is to shard the parameters across multiple
parameter servers. An example of how to do this is shown in the code example at
the bottom.

**Controlling Actor Placement:** The placement of specific actors and tasks on different machines can be
specified by using Ray's support for arbitrary [resource requirements][2].
For example, if the worker requires a GPU, then its remote decorator can be
declared with `@ray.remote(num_gpus=1)`. Arbitrary custom resources can be defined
as well.

## Under the Hood

**Dynamic Task Graphs:** Under the hood, remote function invocations and actor
method invocations create tasks that are added to a dynamically growing graph of
tasks. The Ray backend is in charge of scheduling and executing these tasks
across a cluster (or a single multicore machine). Tasks can be created by the
"driver" application or by other tasks.

**Data:** Ray efficiently serializes data using the [Apache Arrow][9] data
layout. Objects are shared between workers and actors on the same machine
through [shared memory][10], which avoids the need for copies or
deserialization. This optimization is absolutely critical for achieving good
performance.

**Scheduling:** Ray uses a distributed scheduling approach. Each machine has its
own scheduler, which manages the workers and actors on that machine. Tasks are
submitted by applications and workers to the scheduler on the same machine. From
there, they can be reassigned to other workers or passed to other local
schedulers. This allows Ray to achieve substantially higher task throughput than
what can be achieved with a centralized scheduler, which is important for
machine learning applications.

## Conclusion

A parameter server is normally implemented and shipped as a standalone system.
The thing that makes this approach so powerful is that we're able to implement a
parameter server with a few lines of code as an application. *This approach
makes it much simpler to deploy applications using a parameter server and to
modify the behavior of the parameter server.* For example, if we want to shard
the parameter server, change the update rule, switch between asynchronous and
synchronous updates, ignore straggler workers, or any number of other
customizations, we can do each of these things with a few extra lines of code.

## Running this Code

To run the code below, first install Ray with `pip install ray`. Then you should
be able to run the code below, which implements a sharded parameter server.

```python
import numpy as np
import ray
import time

# Start Ray.
ray.init()


@ray.remote
class ParameterServer(object):
    def __init__(self, dim):
        # Alternatively, params could be a dictionary
        # mapping keys to numpy arrays.
        self.params = np.zeros(dim)

    def get_params(self):
        return self.params

    def update_params(self, grad):
        self.params += grad


@ray.remote
def worker(*parameter_servers):
    for _ in range(100):
        # Get the latest parameters.
        parameter_shards = ray.get(
          [ps.get_params.remote() for ps in parameter_servers])
        params = np.concatenate(parameter_shards)

        # Compute a gradient update. Here we just make a fake
        # update, but in practice this would use a library like
        # TensorFlow and would also take in a batch of data.
        grad = np.ones(10)
        time.sleep(0.2)
        grad_shards = np.split(grad, len(parameter_servers))

        # Send the gradient updates to the parameter servers.
        for ps, grad in zip(parameter_servers, grad_shards):
            ps.update_params.remote(grad)


# Start two parameter servers, each with half of the parameters.
parameter_servers = [ParameterServer.remote(5) for _ in range(2)]

# Start 2 workers.
workers = [worker.remote(*parameter_servers) for _ in range(2)]
```

## Read More

For more information about Ray, take a look at the following links:
1. [The Ray documentation][6]
2. [The Ray API][7]
3. [Fast **serialization** with Ray and Apache Arrow][9]
4. [A paper describing the **Ray system**][11]
5. [Efficient **hyperparameter tuning** with Ray][5]
6. [Scalable **reinforcement learning** with Ray][4]
7. [Speeding up **Pandas** with Ray][8]

Questions should be directed to *ray-dev@googlegroups.com*.


[1]: https://github.com/ray-project/ray
[2]: http://ray.readthedocs.io/en/latest/resources.html
[3]: http://www.sysml.cc/doc/206.pdf
[4]: http://ray.readthedocs.io/en/latest/rllib.html
[5]: http://ray.readthedocs.io/en/latest/tune.html
[6]: http://ray.readthedocs.io/en/latest
[7]: http://ray.readthedocs.io/en/latest/api.html
[8]: https://github.com/modin-project/modin
[9]: https://ray-project.github.io/2017/10/15/fast-python-serialization-with-ray-and-arrow.html
[10]: https://ray-project.github.io/2017/08/07/plasma-in-memory-object-store.html
[11]: https://arxiv.org/abs/1712.05889
