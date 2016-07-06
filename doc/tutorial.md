## Tutorial

This section assumes that Ray has been built. See the [instructions for
installing Ray](download-and-setup.md)

### Trying it out

Start a shell by running this command.
```
python scripts/shell.py
```
By default, this will start up several things

- 1 scheduler (for assigning tasks to workers)
- 1 object store (for sharing objects between worker processes)
- 10 workers (for executing tasks)
- 1 driver (for submitting tasks to the scheduler)

Each of the above items, and each worker, is its own process.

The shell that you just started is the driver process.

You can take a Python object and store it in the object store using `ray.put`.
This turns it into a **remote object** (we are currently on a single machine,
but the terminology makes sense if we are on a cluster), and allows it to be
shared among the worker processes. The function `ray.put` returns an object
reference that is used to identify this remote object.

```python
>>> xref = ray.put([1, 2, 3])
```

We can use `ray.get` to retrieve the object corresponding to an object
reference.

```python
>>> ray.get(xref)
[1, 2, 3]
```
We can call a remote function.
```python
>>> ref = example_functions.increment(1)
>>>ray.get(ref)
2
```

Note that `example_functions.increment` is defined in
[`scripts/example_functions.py`](../scripts/example_functions.py) as

```python
@ray.remote([int], [int])
def increment(x):
  return x + 1
```

Note that, we can pass arguments into remote functions either by value or by
object reference. That is, these two lines have the same behavior.

```python
>>> ray.get(example_functions.increment(1))
2
>>> ray.get(example_functions.increment(ray.put(1)))
2
```
This is convenient for chaining remote functions together, for example,
```python
>>> ref = example_functions.increment(1)
>>> ref = example_functions.increment(ref)
>>> ref = example_functions.increment(ref)
>>> ray.get(ref)
4
```

### Visualize the computation graph

At any point, we can visualize the computation graph by running
```python
>>> ray.visualize_computation_graph(view=True)
```
This will display an image like the following one.

<p align="center">
  <img src="figures/compgraph1.png" width="300">
</p>

### Restart workers

During development, suppose that you want to change the implementation of
`example_functions.increment`, but you've already done a bunch of work in the
shell loading and preprocessing data, and you don't want to have to recompute
all of that work.

We can simply restart the workers.

First, change the code, for example, modify the function
`example_functions.increment` in
[`scripts/example_functions.py`](../scripts/example_functions.py) to add 10
instead of 1.

```python
@ray.remote([int], [int])
def increment(x):
  return x + 10
```
Then from the shell, restart the workers like this.
```python
>>> ray.restart_workers("scripts/example_worker.py") # This should be the correct relative path to the example_worker.py code
```
We can check that the code has been updated by running.
```python
>>> ray.get(example_functions.increment(1))
11
```

Note that it is not as simple as running `reload(example_functions)` because we
need to reload the Python module on all of the workers as well, and the workers
are separate Python processes. Calling `reload(example_functions)` would only
reload the module on the driver.
