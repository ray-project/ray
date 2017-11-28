---
layout: post
title: "A Parameter Server in Fifteen Lines of Python"
excerpt: "This post describes how to implement a parameter server in Ray."
date: 2017-11-27 14:00:00
---

This post describes how to use [Ray][1] to implement a parameter server in a few
lines of code.

## Parameter Servers

A parameter server is a key-value store. The **values** are the parameters of a
machine-learning model (e.g., a neural network). The **keys** index the model
parameters. Some **examples**:

- In a **recommendation system**, there may be one key per user and one key per
  movie. For each user and movie, there are corresponding user-specific and
  movie-specific parameters.
- In a **language-modeling** application, there may be one key per word, and one
  vector of parameters per word.
- In an **object-recognition** example, there may be one key per layer of a
  neural network and the corresponding values are the parameters of the layer.
- In a small **neural network**, there may be only a single key and the
  corresponding value consists of all of the network's parameters.

As a programming object, a parameter server exposes one method for **getting**
parameters and one method for **updating** parameters.

A parameter server typically exists as a remote process or service, and
interacts with clients through remote procedure calls. As such, it is a very
natural fit for **Ray's actor model**.

**A parameter server can be implemented as a Ray actor as follows (15 lines).**

```python
@ray.remote
class ParameterServer(object):
    def __init__(self, keys, values):
        # These values will be mutated, so we must create a local copy.
        values = [value.copy() for value in values]
        self.parameters = dict(zip(keys, values))

    def get(self, keys):
        return [self.parameters[key] for key in keys]

    def update(self, keys, values):
        # This update function adds to the existing values, but the update
        # function can be defined arbitrarily.
        for key, value in zip(keys, values):
            self.parameters[key] += value
```

This example is taken from a [simple distributed training example][2].

To instantiate the parameter server, do the following.

```python
import numpy as np

initial_keys = ['key1', 'key2', 'key3']
initial_values = [np.random.normal(size=(10, 10)) for _ in range(3)]
parameter_server = ParameterServer.remote(initial_keys, initial_values)
```

To create four long-running workers that continuously retrieve and update the
parameters, do the following.

```python
@ray.remote
def worker_task(parameter_server):
    while True:
        keys = ['key1', 'key2', 'key3']
        # Get the latest parameters.
        values = ray.get(parameter_server.get.remote(keys))
        # Compute some parameter updates.
        updates = ...
        # Update the parameters.
        parameter_server.update.remote(keys, updates)

# Start 4 long-running tasks.
for _ in range(4):
    worker_task.remote(parameter_server)
```

## Sharding Across Multiple Parameter Servers

When your parameters are large and your cluster is large, a single parameter
server may not suffice because the application could be bottlenecked by the
network bandwidth into and out of the machine that the parameter server is on.

To shard the parameters across four parameter servers, do the following.

```python
parameter_servers = [ParameterServer.remote(initial_keys, initial_values)
                     for _ in range(4)]
```

## Controlling Parameter Server Placement

- Example using labels


## Benchmarks

- Single machine benchmarks
- Multi-machine benchmarks
- Compare to TensorFlow

[1]: http://ray.readthedocs.io/en/latest/index.html
[2]: http://ray.readthedocs.io/en/latest/example-parameter-server.html
