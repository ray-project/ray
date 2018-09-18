Resource (CPUs, GPUs)
=====================

This document describes how resources are managed in Ray. Each node in a Ray
cluster knows its own resource capacities, and each task specifies its resource
requirements.

CPUs and GPUs
-------------

The Ray backend includes built-in support for CPUs and GPUs.

Specifying a node's resource requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specify a node's resource requirements from the command line, pass the
``--num-cpus`` and ``--num-cpus`` flags into ``ray start``.

.. code-block:: bash

  # To start a head node.
  ray start --head --num-cpus=8 --num-gpus=1

  # To start a non-head node.
  ray start --redis-address=<redis-address> --num-cpus=4 --num-gpus=2

To specify a node's resource requirements when the Ray processes are all started
through ``ray.init``, do the following.

.. code-block:: python

  ray.init(num_cpus=8, num_gpus=1)

If the number of CPUs is unspecified, Ray will automatically determine the
number by running ``multiprocessing.cpu_count()``. If the number of GPUs is
unspecified, Ray will attempt to automatically detect the number of GPUs.

Specifying a task's CPU and GPU requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To specify a task's CPU and GPU requirements, pass the ``num_cpus`` and
``num_gpus`` arguments into the remote decorator.

.. code-block:: python

  @ray.remote(num_cpus=4, num_gpus=2)
  def f():
      return 1

When ``f`` tasks will be scheduled on machines that have at least 4 CPUs and 2
GPUs, and when one of the ``f`` tasks executes, 4 CPUs and 2 GPUs will be
reserved for that task. The IDs of the GPUs that are reserved for the task can
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

When an ``Actor`` instance is created, it will be placed on a node that has at
least 1 GPU, and the GPU will be reserved for the actor for the duration of the
actor's lifetime (even if the actor is not executing tasks). The GPU resources
will be released when the actor terminates. Note that currently **only GPU
resources are used for actor placement**.

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
