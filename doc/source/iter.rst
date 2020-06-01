Parallel Iterators
=====================

.. _`issue on GitHub`: https://github.com/ray-project/ray/issues

``ray.util.iter`` provides a parallel iterator API for simple data ingest and processing. It can be thought of as syntactic sugar around Ray actors and ``ray.wait`` loops.

Parallel iterators are lazy and can operate over infinite sequences of items. Iterator
transformations are only executed when the user calls ``next()`` to fetch the next output
item from the iterator.

.. note::

  This API is new and may be revised in future Ray releases. If you encounter
  any bugs, please file an `issue on GitHub`_.

Concepts
--------

**Parallel Iterators**: You can create a ``ParallelIterator`` object from an existing
set of items, range of numbers, set of iterators, or set of worker actors. Ray will
create a worker actor that produces the data for each shard of the iterator:

.. code-block:: python

    # Create an iterator with 2 worker actors over the list [1, 2, 3, 4].
    >>> it = ray.util.iter.from_items([1, 2, 3, 4], num_shards=2)
    ParallelIterator[from_items[int, 4, shards=2]]

    # Create an iterator with 32 worker actors over range(1000000).
    >>> it = ray.util.iter.from_range(1000000, num_shards=32)
    ParallelIterator[from_range[1000000, shards=32]]

    # Create an iterator over two range(10) generators.
    >>> it = ray.util.iter.from_iterators([range(10), range(10)])
    ParallelIterator[from_iterators[shards=2]]

    # Create an iterator from existing worker actors. These actors must
    # implement the ParallelIteratorWorker interface.
    >>> it = ray.util.iter.from_actors([a1, a2, a3, a4])
    ParallelIterator[from_actors[shards=4]]

Simple transformations can be chained on the iterator, such as mapping,
filtering, and batching. These will be executed in parallel on the workers:

.. code-block:: python

    # Apply a transformation to each element of the iterator.
    >>> it = it.for_each(lambda x: x ** 2)
    ParallelIterator[...].for_each()

    # Batch together items into a lists of 32 elements.
    >>> it = it.batch(32)
    ParallelIterator[...].for_each().batch(32)

    # Filter out items with odd values.
    >>> it = it.filter(lambda x: x % 2 == 0)
    ParallelIterator[...].for_each().batch(32).filter()

**Local Iterators**: To read elements from a parallel iterator, it has to be converted
to a ``LocalIterator`` by calling ``gather_sync()`` or ``gather_async()``. These
correspond to ``ray.get`` and ``ray.wait`` loops over the actors respectively:

.. code-block:: python

    # Gather items synchronously (deterministic round robin across shards):
    >>> it = ray.util.iter.from_range(1000000, 1)
    >>> it = it.gather_sync()
    LocalIterator[ParallelIterator[from_range[1000000, shards=1]].gather_sync()]

    # Local iterators can be used as any other Python iterator.
    >>> it.take(5)
    [0, 1, 2, 3, 4]

    # They also support chaining of transformations. Unlike transformations
    # applied on a ParallelIterator, they will be executed in the current process.
    >>> it.filter(lambda x: x % 2 == 0).take(5)
    [0, 2, 4, 6, 8]

    # Async gather can be used for better performance, but it is non-deterministic.
    >>> it = ray.util.iter.from_range(1000, 4).gather_async()
    >>> it.take(5)
    [0, 250, 500, 750, 1]

**Passing iterators to remote functions**: Both ``ParallelIterator`` and ``LocalIterator``
are serializable. They can be passed to any Ray remote function. However, note that
each shard should only be read by one process at a time:

.. code-block:: python

    # Get local iterators representing the shards of this ParallelIterator:
    >>> it = ray.util.iter.from_range(10000, 3)
    >>> [s0, s1, s2] = it.shards()
    [LocalIterator[from_range[10000, shards=3].shard[0]],
     LocalIterator[from_range[10000, shards=3].shard[1]],
     LocalIterator[from_range[10000, shards=3].shard[2]]]

    # Iterator shards can be passed to remote functions.
    >>> @ray.remote
    ... def do_sum(it):
    ...     return sum(it)
    ...
    >>> ray.get([do_sum.remote(s) for s in it.shards()])
    [5552778, 16661667, 27780555]

Semantic Guarantees
~~~~~~~~~~~~~~~~~~~

The parallel iterator API guarantees the following semantics:

**Fetch ordering**: When using ``it.gather_sync().foreach(fn)`` or
``it.gather_async().foreach(fn)`` (or any other transformation after a gather),
``fn(x_i)`` will be called on the element ``x_i`` before the next
element ``x_{i+1}`` is fetched from the source actor. This is useful if you need to
update the source actor between iterator steps. Note that for async gather, this
ordering only applies per shard.

**Operator state**: Operator state is preserved for each shard.
This means that you can pass a stateful callable to ``.foreach()``:

.. code-block:: python

    class CumulativeSum:
        def __init__(self):
            self.total = 0

        def __call__(self, x):
            self.total += x
            return (self.total, x)

    it = ray.util.iter.from_range(5, 1)
    for x in it.for_each(CumulativeSum()).gather_sync():
        print(x)

    ## This prints:
    #(0, 0)
    #(1, 1)
    #(3, 2)
    #(6, 3)
    #(10, 4)

Example: Streaming word frequency count
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Parallel iterators can be used for simple data processing use cases such as
streaming grep:

.. code-block:: python

    import ray
    import glob
    import gzip
    import numpy as np

    ray.init()

    file_list = glob.glob("/var/log/syslog*.gz")
    it = (
        ray.util.iter.from_items(file_list, num_shards=4)
           .for_each(lambda f: gzip.open(f).readlines())
           .flatten()
           .for_each(lambda line: line.decode("utf-8"))
           .for_each(lambda line: 1 if "cron" in line else 0)
           .batch(1024)
           .for_each(np.mean)
    )

    # Show the probability of a log line containing "cron", with a
    # sliding window of 1024 lines.
    for freq in it.gather_async():
        print(freq)

Example: Passing iterator shards to remote functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both parallel iterators and local iterators are fully serializable, so once
created you can pass them to Ray tasks and actors. This can be useful for
distributed training:

.. code-block:: python

    import ray
    import numpy as np

    ray.init()

    @ray.remote
    def train(data_shard):
        for batch in data_shard:
            print("train on", batch)  # perform model update with batch

    it = (
        ray.util.iter.from_range(1000000, num_shards=4, repeat=True)
            .batch(1024)
            .for_each(np.array)
    )

    work = [train.remote(shard) for shard in it.shards()]
    ray.get(work)

API Reference
-------------

.. automodule:: ray.util.iter
    :members:
    :show-inheritance:
