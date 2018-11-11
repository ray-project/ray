Redis Memory Management (Experimental)
======================================

Ray stores metadata associated with tasks and objects in one or more Redis
servers, as described in `An Overview of the Internals
<internals-overview.html>`_.  Applications that are long-running or have high
task/object generation rate could risk high memory pressure, potentially leading
to out-of-memory (OOM) errors.

Here, we describe an experimental feature that transparently flushes metadata
entries out of Redis memory.

Requirements
------------

As of early July 2018, the automatic memory management feature requires building
Ray from source.  We are planning on eliminating this step in the near future by
releasing official wheels.

Building Ray
~~~~~~~~~~~~

First, follow `instructions to build Ray from source
<installation.html#building-ray-from-source>`__ to install prerequisites.  After
the prerequisites are installed, instead of doing the regular ``pip install`` as
referenced in that document, pass an additional special flag,
``RAY_USE_NEW_GCS=on``:

.. code-block:: bash

  git clone https://github.com/ray-project/ray.git
  cd ray/python
  RAY_USE_NEW_GCS=on pip install -e . --verbose  # Add --user if you see a permission denied error.

Running Ray applications
~~~~~~~~~~~~~~~~~~~~~~~~

At run time the environment variables ``RAY_USE_NEW_GCS=on`` and
``RAY_USE_XRAY=1`` are required.

.. code-block:: bash

  export RAY_USE_NEW_GCS=on
  export RAY_USE_XRAY=1
  python my_ray_script.py  # Or launch python/ipython.

Activate memory flushing
------------------------

After building Ray using the method above, simply add these two lines after
``ray.init()`` to activate automatic memory flushing:

.. code-block:: python

   ray.init(...)

   policy = ray.experimental.SimpleGcsFlushPolicy()
   ray.experimental.set_flushing_policy(policy)

   # My awesome Ray application logic follows.

Paramaters of the flushing policy
---------------------------------

There are three `user-configurable parameters
<https://github.com/ray-project/ray/blob/8190ff1fd0c4b82f73e2c1c0f21de6bda494718c/python/ray/experimental/gcs_flush_policy.py#L31>`_
of the ``SimpleGcsFlushPolicy``:

* ``flush_when_at_least_bytes``: Wait until this many bytes of memory usage
  accumulated in the redis server before flushing kicks in.
* ``flush_period_secs``: Issue a flush to the Redis server every this many
  seconds.
* ``flush_num_entries_each_time``: A hint to the system on the number of entries
  to flush on each request.

The default values should serve to be non-invasive for lightweight Ray
applications. ``flush_when_at_least_bytes`` is set to ``(1<<31)`` or 2GB,
``flush_period_secs`` to 10, and ``flush_num_entries_each_time`` to 10000:

.. code-block:: python

    # Default parameters.
    ray.experimental.SimpleGcsFlushPolicy(
        flush_when_at_least_bytes=(1 << 31),
        flush_period_secs=10,
        flush_num_entries_each_time=10000)

In particular, these default values imply that

1. the Redis server would accumulate memory usage up to 2GB without any entries
being flushed, then the flushing would kick in; and

2. generally, "older" metadata entries would be flushed first, and the Redis
server would always keep the most recent window of metadata of 2GB in size.

**For advanced users.** Advanced users can tune the above parameters to their
applications' needs; note that the desired flush rate is equal to (flush
period) * (num entries each flush).
