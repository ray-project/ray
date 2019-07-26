Specifying Resources
====================

Often times, you might want to load balance your Ray program, not placing all functions and actors on one machine.
This is especially important with GPUs, where one actor may require an entire GPU to complete its task.

When calling ``ray.init()`` without connecting to an existing Ray cluster, Ray will automatically detect the available GPUs and CPUs on the machine.

To specify a task's CPU and GPU requirements, pass the ``num_cpus`` and ``num_gpus`` arguments into the remote decorator.
The task will only run on a machine if there are enough CPU and GPU (and other custom) resources available to execute the task.

Notes:

 * Ray supports **fractional** resource requirements.
 * If specifying CPUs, Ray does not enforce isolation (i.e., your task is expected to honor its request.)
 * If specifying GPUs, Ray does provide isolation in forms of visible devices (``CUDA_VISIBLE_DEVICES``)

.. code-block:: python

  @ray.remote(num_cpus=4, num_gpus=2)
  def f():
      return 1

  @ray.remote(num_gpus=0.5)
  def h():
      return 1

The ``f`` tasks will be scheduled on machines that have at least 4 CPUs and 2
GPUs, and when one of the ``f`` tasks executes, 4 CPUs and 2 GPUs will be
reserved for the duration of the task. We will cover how available resources are determined in the Ray cluster below.

The IDs of the GPUs that are reserved for the task can
be accessed with ``ray.get_gpu_ids()``. Ray will automatically set the
environment variable ``CUDA_VISIBLE_DEVICES`` for that process. These resources
will be released when the task finishes executing.

However, if the task gets blocked in a call to ``ray.get``. For example,
consider the following remote function.

.. code-block:: python

  @ray.remote(num_cpus=1, num_gpus=1)
  def g():
      return ray.get(f.remote())

When a ``g`` task is executing, it will release its CPU resources when it gets
blocked in the call to ``ray.get``. It will reacquire the CPU resources when
``ray.get`` returns. It will retain its GPU resources throughout the lifetime of
the task because the task will most likely continue to use GPU memory.

To specify that an **actor** requires GPUs, do the following.

.. code-block:: python

  @ray.remote(num_gpus=1)
  class Actor(object):
      pass

.. note::

    TODO : When an ``Actor`` instance is created, it will be placed on a node that has at least 1 GPU, and the GPU will be reserved for the actor for the duration of the actor's lifetime (even if the actor is not executing tasks). The GPU resources will be released when the actor terminates. Note that currently **only GPU resources are used for actor placement**.

Cluster Resources
-----------------

The Ray backend includes built-in support for CPUs and GPUs.

Specifying a cluster's resource
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, available resource are detected. However, to override the available resources, pass the
``--num-cpus`` and ``--num-cpus`` flags into ``ray start``.

.. code-block:: bash

  # To start a head node.
  $ ray start --head --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # To start a non-head node.
  $ ray start --redis-address=<redis-address> --num-cpus=<NUM_CPUS> --num-gpus=<NUM_GPUS>

  # Connect to ray. Notice if connected to existing cluster, you don't specify resources.
  ray.init(redis_address=<redis-address>)

  # If not connecting to an existing cluster, you can specify resources:
  ray.init(num_cpus=8, num_gpus=1)

Custom Resources
----------------

While Ray has built-in support for CPUs and GPUs, nodes can be started with
arbitrary custom resources. **All custom resources behave like GPUs.**

A node can be started with some custom resources as follows.

.. code-block:: bash

  ray start --head --resources='{"Resource1": 4, "Resource2": 16}'

It can be done through ``ray.init`` as follows.

.. code-block:: python

  ray.init(resources={'Resource1': 4, 'Resource2': 16})

To require custom resources in a task, specify the requirements in the remote
decorator.

.. code-block:: python

  @ray.remote(resources={'Resource2': 1})
  def f():
      return 1

