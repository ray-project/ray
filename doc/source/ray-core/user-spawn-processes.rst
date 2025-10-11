Lifetimes of a User-Spawn Process
=================================

When you spawn child processes from Ray workers, you are responsible for managing the lifetime of child processes. However, it is not always possible, especially when worker crashes and child processes are spawned from libraries (torch dataloader).

To avoid leaking user-spawned processes, Ray provides mechanisms to kill all user-spawned processes when a worker that starts it exits. This feature prevents GPU memory leaks from child processes (e.g., torch).

Ray provides following mechanisms to handle subprocess killing on worker exit:

- ``RAY_kill_child_processes_on_worker_exit`` (default ``true``): Only works on Linux. If true, the worker kills all *direct* child processes on exit. This won't work if the worker crashed. This is NOT recursive, in that grandchild processes are not killed by this mechanism.

- ``RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper`` (default ``false``): Only works on Linux greater than or equal to 3.4. If true, Raylet *recursively* kills any child processes and grandchild processes that were spawned by the worker after the worker exits. This works even if the worker crashed. The killing happens within 10 seconds after the worker death.

- ``RAY_process_group_cleanup_enabled`` (default ``false``): If true (POSIX), Ray isolates each worker into its own process group at spawn and cleans up the worker’s process group on worker exit via `killpg`. Processes that intentionally call `setsid()` will detach and not be killed by this cleanup.

On non-Linux platforms, subreaper is not available. Per‑worker process groups are supported on POSIX platforms; on Windows, neither subreaper nor PGs apply. Users should manage child processes explicitly on platforms without support.

Note: The feature is meant to be a last resort to kill orphaned processes. It is not a replacement for proper process management. Users should still manage the lifetime of their processes and clean up properly.

.. contents::
  :local:

User-Spawned Process Killed on Worker Exit
------------------------------------------

The following example uses a Ray Actor to spawn a user process. The user process is a sleep process.

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

    def signal_my_pid(self):
      import signal
      os.kill(os.getpid(), signal.SIGKILL)


  actor = MyActor.remote()

  pid = ray.get(actor.start.remote())
  assert psutil.pid_exists(pid)  # the subprocess running

  actor.signal_my_pid.remote()  # sigkill'ed, the worker's subprocess killing no longer works
  time.sleep(11)  # raylet kills orphans every 10s
  assert not psutil.pid_exists(pid)


Enabling the feature
-------------------------

To enable the subreaper feature (deprecated), set via `_system_config` or equivalent cluster configuration at start. You must restart the cluster to apply the change. Prefer enabling `process_group_cleanup_enabled` instead.

.. code-block:: bash

  RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper=true ray start --head

Another way is to enable it during ``ray.init()`` by adding a ``_system_config`` like this:

.. code-block::

  ray.init(_system_config={"kill_child_processes_on_worker_exit_with_raylet_subreaper":True})


⚠️ Caution: Core worker now reaps zombies, toggle back if you wait to ``waitpid``
----------------------------------------------------------------------------------

When subreaper is enabled, the worker process also becomes a subreaper (Linux), meaning some grandchildren processes can be reparented to the worker process. The worker sets ``SIGCHLD`` to ``SIG_IGN``. If you need to wait for a child process to exit, reset ``SIGCHLD`` to ``SIG_DFL`` first.

.. code-block::

  import signal
  signal.signal(signal.SIGCHLD, signal.SIG_DFL)


Under the hood
-------------------------

This feature is implemented by setting the `prctl(PR_SET_CHILD_SUBREAPER, 1)` flag on the Raylet process which spawns all Ray workers. See `prctl(2) <https://man7.org/linux/man-pages/man2/prctl.2.html>`_. This flag makes the Raylet process a "subreaper" which means that if a descendant child process dies, the dead child's children processes reparent to the Raylet process. Subreaper is deprecated in favor of per‑worker process groups.

Raylet maintains a list of "known" direct children pid it spawns, and when the Raylet process receives the SIGCHLD signal, it knows that one of its child processes (e.g. the workers) has died, and maybe there are reparented orphan processes. Raylet lists all children pids (with ppid = raylet pid), and if a child pid is not "known" (i.e. not in the list of direct children pids), Raylet thinks it is an orphan process and kills it via `SIGKILL`.

For a deep chain of process creations, Raylet would do the killing step by step. For example, in a chain like this:

.. code-block::

  raylet -> the worker -> user process A -> user process B -> user process C

When the ``the worker`` dies, ``Raylet`` kills the ``user process A``, because it's not on the "known" children list. When ``user process A`` dies, ``Raylet`` kills ``user process B``, and so on.

An edge case is, if the ``the worker`` is still alive but the ``user process A`` is dead, then ``user process B`` gets reparented and risks being killed. To mitigate, ``Ray`` also sets the ``the worker`` as a subreaper, so it can adopt the reparented processes. ``Core worker`` does not kill unknown children processes, so a user "daemon" process e.g. ``user process B`` that outlives ``user process A`` can live along. However if the ``the worker`` dies, the user daemon process gets reparented to ``raylet`` and gets killed.

Related PR: `Use subreaper to kill unowned subprocesses in raylet. (#42992) <https://github.com/ray-project/ray/pull/42992>`_
