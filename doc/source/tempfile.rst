Temporary Files
===============

Ray will produce some temporary files during running.
They are useful for logging, debugging & sharing object store with other programs.

Location of Temporary Files
---------------------------

First we introduce the concept of a session of Ray.

A session contains a set of processes. A session is created by executing
``ray start`` command or call ``ray.init()`` in a Python script and ended by
executing ``ray stop`` or call ``ray.shutdown()``.

For each session, Ray will create a *root temporary directory* to place all its
temporary files. The path is ``/tmp/ray/session_{datetime}_{pid}`` by default.
The pid belongs to the startup process (the process calling ``ray.init()`` or
the Ray process executed by a shell in ``ray start``).
You can sort by their names to find the latest session.

You are allowed to change the *root temporary directory* in one of these ways:

* Pass ``--temp-dir={your temp path}`` to ``ray start``
* Specify ``temp_dir`` when call ``ray.init()``

You can also use ``default_worker.py --temp-dir={your temp path}`` to
start a new worker with given *root temporary directory*.

The *root temporary directory* you specified will be given as it is,
without pids or datetime attached.

Layout of Temporary Files
-------------------------

A typical layout of temporary files could look like this:

.. code-block:: text

  /tmp
  └── ray
      └── session_{datetime}_{pid}
          ├── logs  # for logging
          │   ├── log_monitor.err
          │   ├── log_monitor.out
          │   ├── monitor.err
          │   ├── monitor.out
          │   ├── plasma_store_0.err  # array of plasma stores' outputs
          │   ├── plasma_store_0.out
          │   ├── raylet_0.err
          │   ├── raylet_0.out
          │   ├── redis-shard_0.err   # array of redis shards' outputs
          │   ├── redis-shard_0.out
          │   ├── redis.err  # redis
          │   ├── redis.out
          │   ├── webui.err  # ipython notebook web ui
          │   ├── webui.out
          │   ├── worker-{worker_id}.err  # redirected output of workers
          │   ├── worker-{worker_id}.out
          │   └── {other workers}
          ├── ray_ui.ipynb  # ipython notebook file
          └── sockets  # for sockets
              ├── plasma_store
              └── raylet  # this could be deleted by Ray's shutdown cleanup.


Plasma Object Store Socket
--------------------------

Plasma object store sockets can be used to share objects with other programs using Apache Arrow.

You are allowed to specify the plasma object store socket in one of these ways:

* Pass ``--plasma-store-socket-name={your socket path}`` to ``ray start``
* Specify ``plasma_store_socket_name`` when call ``ray.init()``

The path you specified will be given as it is without being affected any other paths.


Notes
-----

Temporary file policies are defined in ``python/ray/node.py``.
