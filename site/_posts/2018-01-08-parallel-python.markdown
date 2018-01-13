---
layout: post
title: "Parallel and Distributed Python With Ray"
excerpt: "This post describes how to easily parallelize Python code using Ray."
date: 2018-01-08 14:00:00
---

Most approaches to parallelizing Python code are based on multiprocessing as
opposed to multithreading. The reason for this is that Python's [global
interpreter lock][6] severely limits multithreading in many situations.

Many applications that parallelize Python code on a single machine currently
rely on the builtin [multiprocessing][2] module as well as enhancements like
[joblib][7]

This post is about a new framework [Ray][1], which improves on these existing
tools. At its simplest, Ray can be used as a library for high-performance
multiprocessing on a single machine. On the other end of the spectrum, Ray can
be used as a fault-tolerant distributed system running on hundreds of machines
and tens of thousands of cores.

#### Performance Advantages

1. The same code will run on a **single multicore machine** as well as a **large
   cluster**.
2. Numerical data is efficiently shared between workers (on the same machine)
   through **shared memory** and **zero-copy serialization** (read more
   [here][3]).

#### API Advantages

1. The outputs of asynchronous tasks can be passed into other asynchronous
   tasks. Doing so constructs graphs of task dependencies which are taken into
   account in scheduling decisions.
2. Tasks can depend on specific quantities of various **resources** (like GPUs).
   Read more [here][4].
3. In addition to executing tasks in the background, Ray can **instantiate
   classes remotely** (as an actor) so that workers can maintain state that is
   shared between tasks. Read more [here][5].
4. **Error messages** are propagated from remote tasks back to the application.
5. Tasks can submit more tasks, leading to **nested parallelism**.

## How to Parallelize Code

Suppose you have a for loop with some expensive computation that you want to
parallelize.

```python
import time

def f(x):
    # Do a lot of computation here.
    time.sleep(0.1)
    return x

results = []

for i in range(50):
    results.append(f(i))
```

To parallelize this code using Ray, you have to do three things.

- Declare the function with the `@ray.remote` decorator.
- Invoke the function with `.remote`.
- Retrieve the results with `ray.get`.

```python
import ray
import time

ray.init()  # Initialize Ray.

@ray.remote
def f(x):
    # Do a lot of computation here.
    time.sleep(0.1)
    return x

result_ids = []

for i in range(50):
    result_ids.append(f.remote(i))

results = ray.get(result_ids)
```

The tasks will execute in the background. The number of concurrent tasks will be
equal to the number of cores available to Ray.

The same code will run on a multicore machine or on a cluster. The only line that has to
change in the cluster setting is the call to `ray.init()`.

## Handling Task Dependencies

Suppose that some tasks depend on the outputs of other tasks. For example,
suppose that a number of data loading tasks feed into a number of data
processing tasks.

Our first attempt might look something like the following.

```python
import numpy as np
import time

@ray.remote
def load_data(filename):
    # Load some data.
    time.sleep(np.random.uniform(0, 1))
    return np.random.normal(size=(100, 100))

@ray.remote
def processing1(data):
    # Do some processing.
    time.sleep(np.random.uniform(0, 1))
    return data.mean()

@ray.remote
def processing2(data):
    # Do some other processing.
    time.sleep(np.random.uniform(0, 1))
    return data.std()

filenames = ['file' + str(i) for i in range(4)]

# Load some data.
x_ids = []
for filename in filenames:
    x_ids.append(load_data.remote(filename))
xs = ray.get(x_ids)

# Do the processing.
result_ids1 = []
result_ids2 = []
for x in xs:
    result_ids1.append(processing1.remote(x))
    result_ids2.append(processing2.remote(x))

results1 = ray.get(result_ids1)
results2 = ray.get(result_ids2)
```

There are some potential drawbacks here. In particular, as written, all of the
`load_data` tasks must complete before any of the `processing1` or `processing2`
tasks can begin (because the call to `ray.get` is blocking).

However, there is no need to collect all of the data in the "driver" process.
Instead, the output of the data loading tasks can be passed directly to the
processing tasks without passing through the driver (here the term "driver"
refers to the Python process running the overall script). We can express this
behavior as follows.

```python
result_ids1 = []
result_ids2 = []
for filename in filenames:
    x_id = load_data.remote(filename)
    result_ids1.append(processing1.remote(x_id))
    result_ids2.append(processing2.remote(x_id))

# Block and fetch the results.
results1 = ray.get(result_ids1)
results2 = ray.get(result_ids2)
```

By passing `x_id` into the processing tasks, we encode the fact that the
particular processing tasks depend on the task that creates `x_id`. Those
processing tasks will not begin executing until the object corresponding to `x_id`
has been created. Then it will be passed under the hood to the relevant processing
tasks.

This allows the relevant processing tasks to begin executing as soon as their
object dependencies are ready.

## Stateful Computation and Side Effects

What if you care primarily about the side effects of a task and not just the
return value. You may need multiple tasks that operate on the same shared
mutable state. In object-oriented programming, "objects" and "classes" are used
to encapsulate state that is shared between method invocations. The analog in
the context of distributed computing is an "actor". An actor is a stateful
service. Clients can send remote procedure calls to the actor to invoke methods
on the actor which may mutate the actor's state or have other side effects.

In Python, this is implemented with remote classes.

```python
@ray.remote
class LoggingActor(object):
    def __init__(self):
        self.logs = []
        self.counter = 0

    def add_log(self, message):
        self.logs.append(message)

    def read_new_logs(self):
        new_logs = self.logs[self.counter:]
        self.counter = len(self.logs)
        return new_logs
```

To instantiate one copy of the actor, do the following. This creates a remote process
with a copy of the `LoggingActor`.

```python
# Instantiate one copy of the actor.
logger = LoggingActor.remote()
```

Clients (e.g., other tasks or actors) can invoke methods on the actor which are
executed as tasks by the actor.

```python
import time

@ray.remote
def worker(logger, i):
    for j in range(10):
        # Log a message.
        logger.add_log.remote('message {} from worker {}.'.format(j, i))
        time.sleep(1)

# Start several worker tasks that use the logger. Note that we are passing
# a handle to logger actor to the worker tasks. This will allow the worker
# tasks to invoke methods on the actor.
for i in range(4):
    worker.remote(logger, i)
```

The driver can then read the log messages by quering the actor.

```python
for _ in range(10):
    # The driver can also invoke methods on the actor to read its state.
    print(ray.get(logger.read_new_logs.remote()))
    time.sleep(0.5)
```

To instantiate multiple copies of the actor (e.g., to shard the service so as to
improve throughput), simply instantiate the actor class multiple times.

[1]: http://github.com/ray-project/ray
[2]: https://docs.python.org/2/library/multiprocessing.html
[3]: https://ray-project.github.io/2017/10/15/fast-python-serialization-with-ray-and-arrow.html
[4]: http://ray.readthedocs.io/en/latest/resources.html
[5]: http://ray.readthedocs.io/en/latest/actors.html
[6]: https://wiki.python.org/moin/GlobalInterpreterLock
[7]: http://pythonhosted.org/joblib/
