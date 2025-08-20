Lifetimes of a User-Spawn Process
=================================

When you spawn child processes from Ray workers, you are responsible for managing the lifetime of child processes. However, it is not always possible, especially when worker crashes and child processes are spawned from libraries (torch dataloader).

To avoid leaking user-spawned processes, Ray provides mechanisms to kill all user-spawned processes when a worker that starts it exits. This feature prevents GPU memory leaks from child processes (e.g., torch).

Ray uses per‑worker process groups to clean up user‑spawned processes on worker exit. Each worker is started in its own process group; when a worker exits (including ungraceful crashes), Raylet terminates that process group to clean up the entire subprocess tree.

On POSIX platforms, this provides immediate, recursive cleanup. On Windows, behavior is unchanged from previous releases.

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

  # No special config is required for process‑group cleanup.
  ray.init()

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

  actor.signal_my_pid.remote()  # simulate crash; raylet cleans up the worker's process group
  time.sleep(2)
  assert not psutil.pid_exists(pid)


Opting out for long‑lived daemons
---------------------------------

Advanced users can intentionally detach a child process from the worker’s process group (for example by calling ``setsid()`` in the child). Detached processes will not be terminated when the worker exits; this is a supported escape hatch for long‑lived daemons and shared services. If you use this pattern, you are responsible for managing and cleaning up such processes.


.. note::
   Previous Linux-only behavior using the ``subreaper`` mechanism has been deprecated. Ray no longer enables subreapers for orphan cleanup; per‑worker process groups are used instead.


Under the hood
--------------

Raylet starts workers in their own POSIX process groups. When a worker exits, Raylet terminates the worker’s process group (first sending ``SIGTERM``, then escalating to ``SIGKILL`` after a short grace period) to clean up the entire subtree.
