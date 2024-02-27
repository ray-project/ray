Lifetimes of a User-Spawn Process
=================================

To avoid leaking user-spawned processes, Ray automatically kills all user-spawned processes when a worker that starts it exits. This feature prevents GPU memory leaks from child processes (e.g., torch). 

This document describes the lifecycle of a user-spawned process in Ray. A user-spawned process is a process that is started by a user and not by Ray.

On Linux by default (controlled by the environment variable `RAY_kill_child_processes_on_worker_exit`), the lifetime of a user-spawned process is tied to the lifetime of the parent Ray worker process. When the parent Ray worker process exits, for example when actor is killed, all of its child processes are killed as well.

On other platforms, the lifetime of a user-spawned process is not controlled by Ray. The user is responsible for managing the lifetime of the child processes. If the parent Ray worker process dies, the child processes will continue to run.

.. contents::
  :local:

User-Spawned Process Killed on Worker Exit
------------------------------------------

In the following example, we use Ray Actor to spawn a user process. The user process is a long running process that prints "Hello, world!" every second. The user process is killed when the actor is killed.

.. testcode::

  import ray
  import psutil
  import subprocess

  @ray.remote
  class MyActor:
    def __init__(self):
      pass
      
    def start(self):
      # Start a user process
      process = subprocess.Popen(["python", "-c", "import time; while True: print('Hello, world!'); time.sleep(1)"])
      return process.pid

  actor = MyActor.remote()

  pid = ray.get(actor.start.remote())
  # The user process is running
  assert psutil.pid_exists(pid)

  ray.kill(actor)
  time.sleep(1)
  # The user process is killed when the actor is killed
  assert not psutil.pid_exists(pid)


Disabling the feature
-------------------------

To disable the feature, set the environment variable ``RAY_kill_child_processes_on_worker_exit`` to ``false`` **when starting the Ray cluster**, for example in  If a Ray cluster is already running, you need to restart the Ray cluster to apply the change. Setting ``env_var`` in a runtime environment will NOT work.

.. code-block:: bash

  RAY_kill_child_processes_on_worker_exit=false ray start --head


Platform support
-------------------------

The feature is supported on Linux 3.4 or newer. On other platforms, including Linux<3.4 and MacOS and Windows, the flag is ignored, and the user is responsible for managing the lifetime of the child processes. If the parent Ray worker process dies, the child processes will continue to run, as if the flag is always disabled.


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