---
layout: post
title: "Announcing Ray: An Open-Source Distributed Execution Framework for AI Applications"
date: 2017-05-17 09:51:34 -0700
categories: announcements
---
Ray provides a simple and efficient mechanism for running Python code on
clusters or large multicore machines.

<div align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/07eIebEk1MM?autoplay=1" frameborder="0" allowfullscreen></iframe>
<div>A simulated robot trained to run using Ray.</div>
</div>

# Simple Parallelization of Existing Code

Ray enables Python functions to be executed remotely with minimal modifications.


{% highlight python %}
  import time

  def f():
      time.sleep(1)

  # Tasks executed serially.
  %time [f() for _ in range(10)]  # 10s

  @ray.remote
  def f():
      time.sleep(1)

  # Tasks executed in parallel.
  %time ray.get([f.remote() for _ in range(10)])  # 1s
{% endhighlight %}

# Efficient Shared Memory and Serialization

Serializing and deserializing data is often a bottleneck in distributed
computing. Ray lets worker processes on the same machine access the same objects
through shared memory. To facilitate this, Ray uses an in-memory object store on
each machine to serve objects.

To minimize the time required to deserialize objects in shared memory, we use
the Apache Arrow data layout. For example, suppose we store some neural network
weights in shared memory.

{% highlight python %}
  import numpy as np
  %time weights = {"Variable{}".format(i): np.random.normal(size=5000000)
                   for i in range(10)}  # 2.68s

  # Serialize the weights and copy them into the object store. Then deserialize
  # them.
  %time weights_id = ray.put(weights)      # 0.525s
  %time new_weights = ray.get(weights_id)  # 0.000622s

  # Serialize the weights with pickle. Then deserialize them.
  import pickle
  %time pickled_weights = pickle.dumps(weights)      # 0.986s
  %time new_weights = pickle.loads(pickled_weights)  # 0.241s
{% endhighlight %}

A common pattern is to launch a large number of tasks and then aggregate all of
the results on a single worker. In this case, the ability to quickly deserialize
objects is critical.

Apache Arrow allows us to serialize objects into blobs and to compute offsets
into those blobs without scanning through the blob. This enables us to quickly
deserialize the neural net weights and construct a Python dictionary of numpy
arrays, where each of those numpy arrays is essentially a wrapper around a
pointer to a shared memory buffer in the object store.

# Flexible Encoding of Task Dependencies

In contrast with bulk-synchronous parallel frameworks like MapReduce or Apache
Spark, Ray is designed to support AI applications which require fine-grained
task dependencies. In contrast with the computation of aggregate statistics of
an entire dataset, a training procedure may operate on a small subset of data or
on the outputs of a handful of tasks.

Dependencies can be encoded by passing object IDs (which are the outputs of
tasks) into other tasks.

{% highlight python %}
@ray.remote
def aggregate_data(x, y):
    return x + y

data = range(100)

while len(data) > 1:
  intermediate_result = aggregate_data.remote(data[0], data[1])
  data = data[2:] + [intermediate_result]

result = ray.get(data[0])
{% endhighlight %}

By passing the outputs of some calls to `aggregate_data` into subsequent calls
to `aggregate_data`, we encode that the second task depends on the result of the
first task. If the second task is scheduled on a different machine, the result
of the first task will be transferred to the machine where it is needed. Note
that when object IDs are passed into remote function calls, the actual values
will be unpacked before the function is executed, so when the `aggregate_data`
function is executed, `x` and `y` will be integers.

* Shared Mutable State with Actors

Ray uses actors to share mutable state between tasks. Here is an example in
which multiple tasks share the state of an Atari simulator. Each task runs the
simulator for several steps picking up where the previous task left off.

{% highlight python %}
import gym

@ray.remote
class Simulator(object):
    def __init__(self):
        self.env = gym.make("Pong-v0")
        self.env.reset()

    def step(self, action):
        return self.env.step(action)

# Create a simulator, this will start a new worker that will run all
# methods for this actor.
simulator = Simulator.remote()

observations = []
for _ in range(10):
    # Take action 0 in the simulator.
    observations.append(simulator.step.remote(0))
{% endhighlight %}

Each call to `simulator.step.remote` generates a task that is scheduled on the
actor. These tasks mutate the state of the simulator object, and they are
executed one at a time.
