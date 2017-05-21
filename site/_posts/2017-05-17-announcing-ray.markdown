---
layout: post
title: "Ray: A Distributed Execution Framework for AI Applications"
excerpt: "This post announces Ray, a framework for efficiently running Python code on clusters and large mult-core machines."
date: 2017-05-20 14:00:00
---

This post announces Ray, a framework for efficiently running Python code on
clusters and large multi-core machines. The project is open source.
You can check out [the code](https://github.com/ray-project/ray) and
[the documentation](http://ray.readthedocs.io/en/latest/?badge=latest).

Many AI algorithms are computationally intensive and exhibit complex
communication patterns. As a result, many researchers spend most of their
time building custom systems to efficiently distribute their code across
clusters of machines.

However, the resulting systems are often specific to a single algorithm or class
of algorithms. We built Ray to help eliminate a bunch of the redundant
engineering effort that is currently repeated over and over for each new
algorithm. Our hope is that a few basic primitives can be reused to implement
and to efficiently execute a broad range of algorithms and applications.

<div align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/07eIebEk1MM?autoplay=1&loop=1&playlist=07eIebEk1MM" frameborder="0" allowfullscreen></iframe>
<div>A simulated robot learning to run using Ray.</div>
</div>
<br />

# Simple Parallelization of Existing Code

Ray enables Python functions to be executed remotely with minimal modifications.

With **regular Python**, when you call a function, the call blocks until the
function has been executed. This example would take 8 seconds to execute.

{% highlight python %}
def f():
    time.sleep(1)

# Calls to f executed serially.
results = []
for _ in range(8):
    result = f()
    results.append(result)
{% endhighlight %}

**With Ray**, when you call a **remote function**, the call immediately returns
an object ID. A task is then created, scheduled, and executed somewhere in the
cluster. This example would take 1 second to execute.

{% highlight python %}
@ray.remote
def f():
    time.sleep(1)

# Tasks executed in parallel.
results = []
for _ in range(8):
    result = f.remote()
    results.append(result)

results = ray.get(results)
{% endhighlight %}

Note that the only changes are that we add the ``@ray.remote`` decorator to the
function definition, we call the function with ``f.remote()``, and we call
``ray.get`` on the list of object IDs in order to block until the corresponding
tasks have finished executing.

<div align="center">
<img src="/assets/announcing_ray/graph1.png">
</div>
<div><i>A graph depicting the tasks and objects in this example. The circles
represent tasks, and the boxes represent objects. There are no arrows between
the 8 separate tasks indicating that all of the tasks can be executed in
parallel.</i></div>
<br />

# Flexible Encoding of Task Dependencies

In contrast with bulk-synchronous parallel frameworks like MapReduce or Apache
Spark, Ray is designed to support AI applications which require fine-grained
task dependencies. In contrast with the computation of aggregate statistics of
an entire dataset, a training procedure may operate on a small subset of data or
on the outputs of a handful of tasks.

Dependencies can be encoded by passing object IDs (which are the outputs of
tasks) into other tasks.

{% highlight python %}
import numpy as np

@ray.remote
def aggregate_data(x, y):
    return x + y

data = [np.random.normal(size=1000) for i in range(4)]

while len(data) > 1:
  intermediate_result = aggregate_data.remote(data[0], data[1])
  data = data[2:] + [intermediate_result]

result = ray.get(data[0])
{% endhighlight %}

By passing the outputs of some calls to `aggregate_data` into subsequent calls
to `aggregate_data`, we encode dependencies between these tasks which can be
used by the system to make scheduling decisions and to coordinate the transfer
of objects. Note that when object IDs are passed into remote function calls, the
actual values will be unpacked before the function is executed, so when the
`aggregate_data` function is executed, `x` and `y` will be numpy arrays.

<div align="center">
<img src="/assets/announcing_ray/graph2.png">
</div>
<div><i>A graph depicting the tasks and objects in this example. The circles
represent tasks, and the boxes represent objects. Arrows point from tasks to the
objects they produce and from objects to the tasks that depend on
them.</i></div>
<br />

# Shared Mutable State with Actors

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
for _ in range(4):
    # Take action 0 in the simulator.
    observations.append(simulator.step.remote(0))
{% endhighlight %}

Each call to `simulator.step.remote` generates a task that is scheduled on the
actor. These tasks mutate the state of the simulator object, and they are
executed one at a time.

Like remote functions, actor methods return object IDs that can be passed into
other tasks and whose values can be retrieved with `ray.get`.

<div align="center">
<img src="/assets/announcing_ray/graph3.png">
</div>
<div><i>A graph depicting the tasks and objects in this example. The circles
represent tasks, and the boxes represent objects. The first task is the actor's
constructor. The thick arrows are used to show that the methods invoked on this
actor share the underlying state of the actor.</i></div>
<br />

# Efficient Shared Memory and Serialization with Apache Arrow

Serializing and deserializing data is often a bottleneck in distributed
computing. Ray lets worker processes on the same machine access the same objects
through shared memory. To facilitate this, Ray uses an in-memory object store on
each machine to serve objects.

To illustrate the problem, suppose we create some neural network weights and
wish to ship them from one Python process to another.

{% highlight python %}
import numpy as np

weights = {"Variable{}".format(i): np.random.normal(size=5000000)
           for i in range(10)}  # 2.68s
{% endhighlight %}

To ship the neural network weights around, we need to first serialize them into
a contiguous blob of bytes. This can be done with standard serialization
libraries like pickle.

{% highlight python %}
import pickle

# Serialize the weights with pickle. Then deserialize them.
pickled_weights = pickle.dumps(weights)      # 0.986s
new_weights = pickle.loads(pickled_weights)  # 0.241s
{% endhighlight %}

The time required for deserialization is particularly important because one of
the most common patterns in machine learning is to aggregate a large number of
values (for example, neural net weights, rollouts, or other values) in a single
process, so the deserialization step could happen hundreds of times in a row.

To minimize the time required to deserialize objects in shared memory, we use
the [Apache Arrow](https://arrow.apache.org/) data layout. This allows us to
compute offsets into the serialized blob without scanning through the entire
blob. **In practice, this can translate into deserialization that is several
orders of magnitude faster**.

{% highlight python %}
# Serialize the weights and copy them into the object store. Then deserialize
# them.
weights_id = ray.put(weights)      # 0.525s
new_weights = ray.get(weights_id)  # 0.000622s
{% endhighlight %}

The call to `ray.put` serializes the weights using Arrow and copies the result
into the object store's memory. The call to `ray.get` then deserializes the
serialized object and constructs a new dictionary of numpy arrays. However, the
underlying arrays backing the numpy arrays live in shared memory and are not
copied into the Python process's heap.

Note that if the call to `ray.get` happens from a different machine, the
relevant serialized object will be copied from a machine where it lives to the
machine where it is needed.

In this example, we call `ray.put` explicitly. However, normally this call would
happen under the hood when a Python object is passed into a remote function or
returned from a remote function.

# Feedback is Appreciated

This project is in its early stages. If you try it out, we'd love to hear your
thoughts and suggestions.
