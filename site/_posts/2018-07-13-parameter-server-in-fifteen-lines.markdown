---
layout: post
title: "A Parameter Server in Fifteen Lines of Python"
excerpt: "This post describes how to implement a parameter server in Ray."
date: 2018-07-13 14:00:00
---

This post describes how to use [Ray][1] to implement a parameter server in a few
lines of code. *Ray is a general-purpose framework for parallel and distributed
Python.*

## Parameter Servers

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

    def get(self):
        return self.params

    def update(self, grad):
        # Here we assume that the update is a gradient, which should be added
        # to the parameter vetor.
        self.params += grad
```

A parameter server typically exists as a remote process or service, and
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
        params = ray.get(ps.get.remote())

        # Compute a gradient update. Here we just make a fake update, but in
        # practice this would use a library like TensorFlow and would also take
        # in a batch of data.
        grad = np.ones(10)
        time.sleep(0.2)

        # Update the parameters.
        ps.update.remote(grad)
```

Then we can start several worker tasks as follows.

```python
# Start 4 workers.
for _ in range(4):
    worker.remote(ps)
```

### What's Happening Under the Hood?

TODO

## Natural Extensions

We describe a few natural parameter server extensions below. More examples are
given in [this paper][3].

### Sharding Across Multiple Parameter Servers

When your parameters are large and your cluster is large, a single parameter
server may not suffice because the application could be bottlenecked by the
network bandwidth into and out of the machine that the parameter server is on
(especially if there are many workers).

A natural solution in this case is to shard the parameters across multiple
parameter servers. An example of how to do this is shown in the code example at
the bottom.

### Controlling Actor Placement

The placement of specific actors and tasks on different machines can be
specified by using Ray's support for arbitrary [resource requirements][2].
For example, if the worker requires a GPU, then its remote decorator can be
declared with `@ray.remote(num_gpus=1)`.

## Conclusion

A parameter server is normally implemented and shipped as a standalone system.
The thing that makes this approach so powerful is that we're able to implement a
parameter server with a few lines of code as an application. Because we
implemented it ourselves, it is vastly more configurable. For example, if we
want to shard the parameter server, change the update rule, switch from
asynchronous to synchronous updates, ignore straggler workers, or any number of
other customizations, we can do each of these things with a few extra lines of
code.

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
        # Alternatively, params could be a dictionary mapping keys to numpy
        # arrays.
        self.params = np.zeros(dim)

    def get(self):
        return self.params

    def update(self, grad):
        # Here we assume that the update is a gradient, which should be added
        # to the parameter vetor.
        self.params += grad


@ray.remote
def worker(*parameter_servers):
    for _ in range(100):
        # Get the latest parameters.
        parameter_shards = ray.get(
          [ps.get.remote() for ps in parameter_servers])
        params = np.concatenate(parameter_shards)

        # Compute a gradient update. Here we just make a fake update, but in
        # practice this would use a library like TensorFlow and would also take
        # in a batch of data.
        grad = np.ones(10)
        time.sleep(0.2)
        grad_shards = np.split(grad, len(parameter_servers))

        # Update the parameters.
        for ps, grad in zip(parameter_servers, grad_shards):
            ps.update.remote(grad)


# Start two parameter servers, each with half of the parameters.
parameter_servers = [ParameterServer.remote(5) for _ in range(2)]

# Start 4 workers.
workers = [worker.remote(*parameter_servers) for _ in range(4)]
```

[1]: https://github.com/ray-project/ray
[2]: http://ray.readthedocs.io/en/latest/resources.html
[3]: http://www.sysml.cc/doc/206.pdf
