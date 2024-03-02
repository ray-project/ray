Lifetimes of a User-Spawn Process
=================================

To avoid leaking user-spawned processes, Ray provides mechanisms to kill all user-spawned processes when a worker that starts it exits. This feature prevents GPU memory leaks from child processes (e.g., torch).

We have 2 environment variables to handle subprocess killing on core worker exit:

- ``RAY_kill_child_processes_on_worker_exit`` (default ``true``): Only works on Linux. If true, core worker kills all subprocesses on exit. This won't work if the core worker crashed or was killed by a signal. If a process is created as a daemon, e.g. double forked, it will not be killed by this mechanism.

- ``RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper`` (default ``false``): Only works on Linux greater than or equal to 3.4. If true, Raylet kills any subprocesses that were spawned by the core worker after the core worker exits. This works even if the core worker crashed or was killed by a signal. Even if a process is created as a daemon, e.g. double forked, it will still be killed by this mechanism. The killing happens within 10 seconds after the core worker death.

Both killings happen recursively, meaning if the core worker spawns user process A, then user process A spawns user process B, on core worker exit (or crash) both A and B are killed.

On non-Linux platforms, user-spawned process is not controlled by Ray. The user is responsible for managing the lifetime of the child processes. If the parent Ray worker process dies, the child processes will continue to run.


.. contents::
  :local:

User-Spawned Process Killed on Worker Exit
------------------------------------------

In the following example, we use Ray Actor to spawn a user process. The user process is a long running process that prints "Hello, world!" every second. The user process is killed when the actor is killed.

.. testcode::

  import ray
  import psutil
  import subprocess
  import time
  import os

  ray.init(_system_config={"kill_child_processes_on_worker_exit_with_raylet_subreaper":True})

  @ray.remote
  class MyActor:
    def __init__(self):
      pass

    def start(self):
      # Start a user process
      process = subprocess.Popen(["/bin/bash", "-c", "sleep 10000"])
      return process.pid

    def suicide(self):
      import signal
      os.kill(os.getpid(), signal.SIGKILL)


  actor = MyActor.remote()

  pid = ray.get(actor.start.remote())
  assert psutil.pid_exists(pid)  # the subprocess running

  actor.suicide.remote()  # sigkill'ed, core worker's subprocess killing no longer works
  time.sleep(11)  # raylet kills orphans every 10s
  assert not psutil.pid_exists(pid)


Enabling the feature
-------------------------

To enable the subreaper feature, set the environment variable ``RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper`` to ``true`` **when starting the Ray cluster**, If a Ray cluster is already running, you need to restart the Ray cluster to apply the change. Setting ``env_var`` in a runtime environment will NOT work.

.. code-block:: bash

  RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper=true ray start --head

Another way is to enable it during ``ray.init()`` by adding a ``_system_config`` like this:

.. code-block::

  ray.init(_system_config={"kill_child_processes_on_worker_exit_with_raylet_subreaper":True})


⚠️ Caution: Core worker needs to reap zombies
----------------------------------------------

When the feature is enabled, the core worker process becomes a subreaper (see the next section), meaning there can be some grandchildren processes that are reparented to the core worker process. If these processes exit, core worker needs to reap them to avoid zombies, even though they are not spawn by core worker. If core worker does not reap them, the zombies will accumulate and eventually cause the system to run out of resources like memory.

You can add this code to the Ray Actors or Tasks to reap zombies, if you choose to enable the feature:

.. code-block::

  import signal
  signal.signal(signal.SIGCHLD, signal.SIG_IGN)


Under the hood
-------------------------

This feature is implemented by setting the `prctl(PR_SET_CHILD_SUBREAPER, 1)` flag on the Raylet process which spawns all Ray workers. See `prctl(2) <https://man7.org/linux/man-pages/man2/prctl.2.html>`_. This flag makes the Raylet process a "subreaper" which means that if a descendant child process dies, the dead child's children processes reparent to the Raylet process.

Raylet maintains a list of "known" direct children pid it spawns, and when the Raylet process receives the SIGCHLD signal, it knows that one of its child processes (e.g. core workers) has died, and maybe there are reparented orphan processes. Raylet lists all children pids (with ppid = raylet pid), and if a child pid is not "known" (i.e. not in the list of direct children pids), Raylet thinks it is an orphan process and kills it via `SIGKILL`.

For a deep chain of process creations, Raylet would do the killing step by step. For example, in a chain like this:

.. code-block::

  raylet -> core worker -> user process A -> user process B -> user process C

When the ``core worker`` dies, ``Raylet`` kills the ``user process A``, because it's not on the "known" children list. When ``user process A`` dies, ``Raylet`` kills ``user process B``, and so on.

An edge case is, if the ``core worker`` is still alive but the ``user process A`` is dead, then ``user process B`` gets reparented and risks being killed. To mitigate, ``Ray`` also sets the ``core worker`` as a subreaper, so it can adopt the reparented processes. ``Core worker`` does not kill unknown children processes, so a user "daemon" process e.g. ``user process B`` that outlives ``user process A`` can live along. However if the ``core worker`` dies, the user daemon process gets reparented to ``raylet`` and gets killed.

Related PR: `Use subreaper to kill unowned subprocesses in raylet. (#42992) <https://github.com/ray-project/ray/pull/42992>`_