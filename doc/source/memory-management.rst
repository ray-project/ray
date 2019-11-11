Memory Management
=================

This page describes how memory management works in Ray, and how you can set memory quotas to ensure memory-intensive applications run predictably and reliably.

Overview
--------

There are several ways that Ray applications use memory:

.. image:: images/memory.svg

Ray system memory: this is memory used internally by Ray
  - **Redis**: memory used for storing task lineage and object metadata. When Redis becomes full, lineage will start to be be LRU evicted, which makes the corresponding objects ineligible for reconstruction on failure.
  - **Raylet**: memory used by the C++ raylet process running on each node. This cannot be controlled, but is usually quite small.

Application memory: this is memory used by your application
  - **Worker heap**: memory used by your application (e.g., in Python code or TensorFlow), best measured as the *resident set size (RSS)* of your application minus its *shared memory usage (SHR)* in commands such as ``top``.
  - **Object store memory**: memory used when your application creates objects in the objects store via ``ray.put`` and when returning values from remote functions. Objects are LRU evicted when the store is full, prioritizing objects that are no longer in scope on the driver or any worker. There is an object store server running on each node.
  - **Object store shared memory**: memory used when your application reads objects via ``ray.get``. Note that if an object is already present on the node, this does not cause additional allocations. This allows large objects to be efficiently shared among many actors and tasks.

By default, Ray will cap the memory used by Redis at ``min(30% of node memory, 10GiB)``, and object store at ``min(10% of node memory, 20GiB)``, leaving half of the remaining memory on the node available for use by worker heap. You can also manually configure this by setting ``redis_max_memory=<bytes>`` and ``object_store_memory=<bytes>`` on Ray init.

It is important to note that these default Redis and object store limits do not address the following issues:

* Actor or task heap usage exceeding the remaining available memory on a node.

* Heavy use of the object store by certain actors or tasks causing objects required by other tasks to be prematurely evicted.

To avoid these potential sources of instability, you can set *memory quotas* to reserve memory for individual actors and tasks.

Heap memory quota
-----------------

When Ray starts, it queries the available memory on a node / container not reserved for Redis and the object store or being used by other applications. This is considered "available memory" that actors and tasks can request memory out of. You can also set ``memory=<bytes>`` on Ray init to tell Ray explicitly how much memory is available.

To tell the Ray scheduler a task or actor requires a certain amount of available memory to run, set the ``memory`` argument. The Ray scheduler will then reserve the specified amount of available memory during scheduling, similar to how it handles CPU and GPU resources:

.. code-block:: python

  # reserve 500MiB of available memory to place this task
  @ray.remote(memory=500 * 1024 * 1024)
  def some_function(x):
      pass

  # reserve 2.5GiB of available memory to place this actor
  @ray.remote(memory=2500 * 1024 * 1024)
  class SomeActor(object):
      def __init__(self, a, b):
          pass

In the above example, the memory quota is specified statically by the decorator, but you can also set them dynamically at runtime using ``_remote()`` as follows:

.. code-block:: python

  # override the memory quota to 100MiB when submitting the task
  some_function._remote(memory=100 * 1024 * 1024, kwargs={"x": 1})

  # override the memory quota to 1GiB when creating the actor
  SomeActor._remote(memory=1000 * 1024 * 1024, kwargs={"a": 1, "b": 2})

**Enforcement**: If an actor exceeds its memory quota, calls to it will throw ``RayOutOfMemoryError`` and it may be killed. Memory quota is currently enforced on a best-effort basis for actors only (but quota is taken into account during scheduling in all cases).

Object store memory quota
-------------------------

Use ``@ray.remote(object_store_memory=<bytes>)`` to cap the amount of memory an actor can use for ``ray.put`` and method call returns. This gives the actor its own LRU queue within the object store of the given size, both protecting its objects from eviction by other actors and preventing it from using more than the specified quota. This quota protects objects from unfair eviction when certain actors are producing objects at a much higher rate than others.

Ray takes this resource into account during scheduling, with the caveat that a node will always reserve ~30% of its object store for global shared use.

For the driver, you can set its object store memory quota with ``driver_object_store_memory``. Setting object store quota is not supported for tasks.

Object store shared memory
--------------------------

Object store memory is also used to map objects returned by ``ray.get`` calls in shared memory. While an object is mapped in this way (i.e., there is a Python reference to the object), it is pinned and cannot be evicted from the object store. However, ray does not provide quota management for this kind of shared memory usage.

Summary
-------

You can set memory quotas to ensure your application runs predictably on any Ray cluster configuration. If you're not sure, you can start with a conservative default configuration like the following and see if any limits are hit:

.. code-block:: python

  @ray.remote(
      memory=2000 * 1024 * 1024,
      object_store_memory=200 * 1024 * 1024)
