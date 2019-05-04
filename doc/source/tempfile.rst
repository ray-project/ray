Temporary Files
===============

Ray will produce some temporary files during running.
They are useful for logging, debugging & sharing object store with other programs.

Ray session
-----------

First we introduce the concept of a Ray session.

A Ray session represents all tasks, processes, and resources managed by Ray. A
session is created by executing the ``ray start`` command or by calling
``ray.init()``, and it is terminated by executing ``ray stop`` or calling
``ray.shutdown()``.

Each Ray session will have a unique name. By default, the name is
``session_{timestamp}_{pid}``. The format of ``timestamp`` is
``%Y-%m-%d_%H-%M-%S_%f`` (See `Python time format <strftime.org>`__ for details);
the pid belongs to the startup process (the process calling ``ray.init()`` or
the Ray process executed by a shell in ``ray start``).

Location of Temporary Files
---------------------------

For each session, Ray will place all its temporary files under the
*session directory*. A *session directory* is a subdirectory of the
*root temporary path* (``/tmp/ray`` by default),
so the default session directory is ``/tmp/ray/{ray_session_name}``.
You can sort by their names to find the latest session.

You are allowed to change the *root temporary directory* in one of these ways:

* Pass ``--temp-dir={your temp path}`` to ``ray start``
* Specify ``temp_dir`` when call ``ray.init()``

You can also use ``default_worker.py --temp-dir={your temp path}`` to
start a new worker with the given *root temporary directory*.

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
          │   ├── plasma_store.err  # outputs of the plasma store
          │   ├── plasma_store.out
          │   ├── raylet.err  # outputs of the raylet process
          │   ├── raylet.out
          │   ├── redis-shard_0.err   # outputs of redis shards
          │   ├── redis-shard_0.out
          │   ├── redis.err  # redis
          │   ├── redis.out
          │   ├── webui.err  # ipython notebook web ui
          │   ├── webui.out
          │   ├── worker-{worker_id}.err  # redirected output of workers
          │   ├── worker-{worker_id}.out
          │   └── {other workers}
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
