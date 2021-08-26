---
layout: post
title: "Implementing A Parameter Server in 15 Lines of Python with Ray"
excerpt: "This post describes how to implement a parameter server in Ray."
date: 2018-07-15 14:00:00
---

Parameter servers are a core part of many machine learning applications. Their
role is to store the *parameters* of a machine learning model (e.g., the weights
of a neural network) and to *serve* them to clients (clients are often workers
that process data and compute updates to the parameters).

Parameter servers (like databases) are normally built and shipped as standalone
systems. This post describes how to use [Ray][1] to implement a parameter server
in a few lines of code.

By turning the parameter server from a "system" into an "application", this
approach makes it orders of magnitude simpler to deploy parameter server
applications. Similarly, by allowing applications and libraries to implement
their own parameter servers, this approach makes the behavior of the parameter
server much more configurable and flexible (since the application can simply
modify the implementation with a few lines of Python).

**What is Ray?** [Ray][1] is a general-purpose framework for parallel and
distributed Python. Ray provides a unified task-parallel and actor abstraction
and achieves high performance through shared memory, zero-copy serialization,
and distributed scheduling. Ray also includes high-performance libraries
targeting AI applications, for example [hyperparameter tuning][5] and
[reinforcement learning][4].

## What is a Parameter Server?

A parameter server is a key-value store used for training machine learning
models on a cluster. The **values** are the parameters of a machine-learning
model (e.g., a neural network). The **keys** index the model parameters.

For example, in a movie **recommendation system**, there may be one key per user
and one key per movie. For each user and movie, there are corresponding
user-specific and movie-specific parameters. In a **language-modeling**
application, words may act as keys and their embeddings may be the values. In
its simplest form, a parameter server may implicitly have a single key and allow
all of the parameters to be retrieved and updated at once. We show how such a
parameter server can be implemented as a Ray actor (15 lines) below.

```python
import numpy as np
import ray


@ray.remote
class ParameterServer(object):
    def __init__(self, dim):
        # Alternatively, params could be a dictionary mapping keys to arrays.
        self.params = np.zeros(dim)

    def get_params(self):
        return self.params

    def update_params(self, grad):
        self.params += grad
```

**The `@ray.remote` decorator defines a service.** It takes the
`ParameterServer` class and allows it to be instantiated as a remote service or
actor.

Here, we assume that the update is a gradient which should be added to the
parameter vector. This is just the simplest possible example, and many different
choices could be made.

**A parameter server typically exists as a remote process or service** and
interacts with clients through remote procedure calls. To instantiate the
parameter server as a remote actor, we can do the following.

```python
# We need to start Ray first.
ray.init()

# Create a parameter server process.
ps = ParameterServer.remote(10)
```

**Actor method invocations return futures.** If we want to retrieve the actual
values, we can use a blocking `ray.get` call. For example,

```python
>>> params_id = ps.get_params.remote()  # This returns a future.

>>> params_id
ObjectID(7268cb8d345ef26632430df6f18cc9690eb6b300)

>>> ray.get(params_id)  # This blocks until the task finishes.
array([0., 0., 0., 0., 0., 0., 0., 0., 0., 0.])
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
import time

# Note that the worker function takes a handle to the parameter server as an
# argument, which allows the worker task to invoke methods on the parameter
# server actor.

@ray.remote
def worker(ps):
    for _ in range(100):
        # Get the latest parameters.
        params_id = ps.get_params.remote()  # This method call is non-blocking
                                            # and returns a future.
        params = ray.get(params_id)  # This is a blocking call which waits for
                                     # the task to finish and gets the results.

        # Compute a gradient update. Here we just make a fake update, but in
        # practice this would use a library like TensorFlow and would also take
        # in a batch of data.
        grad = np.ones(10)
        time.sleep(0.2)  # This is a fake placeholder for some computation.

        # Update the parameters.
        ps.update_params.remote(grad)
```

Then we can start several worker tasks as follows.

```python
# Start 2 workers.
for _ in range(2):
    worker.remote(ps)
```

Then we can retrieve the parameters from the driver process and see that they
are being updated by the workers.

```python
>>> ray.get(ps.get_params.remote())
array([64., 64., 64., 64., 64., 64., 64., 64., 64., 64.])
>>> ray.get(ps.get_params.remote())
array([78., 78., 78., 78., 78., 78., 78., 78., 78., 78.])
```

Part of the value that Ray adds here is that *Ray makes it as easy to start up a
remote service or actor as it is to define a Python class*. Handles to the actor
can be passed around to other actors and tasks to allow arbitrary and intuitive
messaging and communication patterns. Current alternatives are much more
involved. For example, [consider how the equivalent runtime service creation and
service handle passing would be done with GRPC][14].

## Additional Extensions

Here we describe some important modifications to the above design. We describe
additional natural extensions in [this paper][3].

**Sharding Across Multiple Parameter Servers:** When your parameters are large
and your cluster is large, a single parameter server may not suffice because the
application could be bottlenecked by the network bandwidth into and out of the
machine that the parameter server is on (especially if there are many workers).

A natural solution in this case is to shard the parameters across multiple
parameter servers. This can be achieved by simply starting up multiple parameter
server actors. An example of how to do this is shown in the code example at the
bottom.

**Controlling Actor Placement:** The placement of specific actors and tasks on
different machines can be specified by using Ray's support for arbitrary
[resource requirements][2]. For example, if the worker requires a GPU, then its
remote decorator can be declared with `@ray.remote(num_gpus=1)`. Arbitrary
custom resources can be defined as well.

## Unifying Tasks and Actors

Ray supports parameter server applications efficiently in large part due to its
unified task-parallel and actor abstraction.

Popular data processing systems such as [Apache Spark][12] allow stateless tasks
(functions with no side effects) to operate on immutable data. This assumption
simplifies the overall system design and makes it easier for applications to
reason about correctness.

However, mutable state that is shared between many tasks is a recurring theme in
machine learning applications. That state could be the weights of a neural
network, the state of a third-party simulator, or an encapsulation of an
interaction with the physical world.

To support these kinds of applications, Ray introduces an actor abstraction. An
actor will execute methods serially (so there are no concurrency issues), and
each method can arbitrarily mutate the actor's internal state. Methods can be
invoked by other actors and tasks (and even by other applications on the same
cluster).

One thing that makes Ray so powerful is that it *unifies the actor abstraction
with the task-parallel abstraction* inheriting the benefits of both approaches.
Ray uses an underlying dynamic task graph to implement both actors and stateless
tasks in the same framework. As a consequence, these two abstractions are
completely interoperable. Tasks and actors can be created from within other
tasks and actors. Both return futures, which can be passed into other tasks or
actor methods to introduce scheduling and data dependencies. As a result, Ray
applications inherit the best features of both tasks and actors.

## Under the Hood

**Dynamic Task Graphs:** Under the hood, remote function invocations and actor
method invocations create tasks that are added to a dynamically growing graph of
tasks. The Ray backend is in charge of scheduling and executing these tasks
across a cluster (or a single multi-core machine). Tasks can be created by the
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
makes it much simpler to deploy applications that use parameter servers and to
modify the behavior of the parameter server.* For example, if we want to shard
the parameter server, change the update rule, switch between asynchronous and
synchronous updates, ignore straggler workers, or any number of other
customizations, we can do each of these things with a few extra lines of code.

This post describes how to use Ray actors to implement a parameter server.
However, actors are a much more general concept and can be useful for many
applications that involve stateful computation. Examples include logging,
streaming, simulation, model serving, graph processing, and many others.

## Running this Code

To run the complete application, first install Ray with `pip install ray`. Then
you should be able to run the code below, which implements a sharded parameter
server.

```python
import numpy as np
import ray
import time

# Start Ray.
ray.init()


@ray.remote
class ParameterServer(object):
    def __init__(self, dim):
        # Alternatively, params could be a dictionary mapping keys to arrays.
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
        time.sleep(0.2)  # This is a fake placeholder for some computation.
        grad_shards = np.split(grad, len(parameter_servers))

        # Send the gradient updates to the parameter servers.
        for ps, grad in zip(parameter_servers, grad_shards):
            ps.update_params.remote(grad)


# Start two parameter servers, each with half of the parameters.
parameter_servers = [ParameterServer.remote(5) for _ in range(2)]

# Start 2 workers.
workers = [worker.remote(*parameter_servers) for _ in range(2)]

# Inspect the parameters at regular intervals.
for _ in range(5):
    time.sleep(1)
    print(ray.get([ps.get_params.remote() for ps in parameter_servers]))
```

Note that this example focuses on simplicity and that more can be done to
optimize this code.

## Read More

For more information about Ray, take a look at the following links:
1. The Ray [documentation][6]
2. The Ray [API][7]
3. Fast [serialization][9] with Ray and Apache Arrow
4. A [paper][11] describing the Ray system
5. Efficient [hyperparameter][5] tuning with Ray
6. Scalable [reinforcement][4] learning with Ray and [the RLlib paper][13]
7. Speeding up [Pandas][8] with Ray

Questions should be directed to *ray-dev@googlegroups.com*.


[1]: https://github.com/ray-project/ray
[2]: http://docs.ray.io/en/master/resources.html
[3]: http://www.sysml.cc/doc/206.pdf
[4]: http://docs.ray.io/en/master/rllib.html
[5]: http://docs.ray.io/en/master/tune.html
[6]: http://docs.ray.io/en/master
[7]: http://docs.ray.io/en/master/api.html
[8]: https://github.com/modin-project/modin
[9]: https://ray-project.github.io/2017/10/15/fast-python-serialization-with-ray-and-arrow.html
[10]: https://ray-project.github.io/2017/08/08/plasma-in-memory-object-store.html
[11]: https://arxiv.org/abs/1712.05889
[12]: http://spark.apache.org
[13]: https://arxiv.org/abs/1712.09381
[14]: https://grpc.io/docs/tutorials/basic/python.html#defining-the-service
