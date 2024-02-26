Lifetimes of a User-Spawn Process
=================================

This document describes the lifecycle of a user-spawned process in Ray. A user-spawned process is a process that is started by a user and not by Ray.

On Linux by default (controlled by the environment variable `RAY_kill_child_processes_on_worker_exit`), the lifetime of a user-spawned process is tied to the lifetime of the parent Ray worker process. When the parent Ray worker process dies, that is, after a task finishes or an actor is killed, all of its child processes are killed as well.

On other platforms, the lifetime of a user-spawned process is not controlled by Ray. The user is responsible for managing the lifetime of the child processes. If the parent Ray worker process dies, the child processes will continue to run.

.. contents::
  :local:

User-Spawned Process Killed on Worker Exit
------------------------------------------

In the following example, we use Ray Actor to spawn a user process. The user process is a long running process that prints "Hello, world!" every second. The user process is killed when the actor is killed.

.. code-block:: python

  import ray
  import psutil
  import subprocess

  @ray.remote
  class MyActor:
    def __init__(self):
      pass
      
    def start(self):
      # Start a user process
      self.process = subprocess.Popen(["python", "-c", "import time; while True: print('Hello, world!'); time.sleep(1)"])
      return self.process.pid

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

The feature is supported on Linux 3.4 or newer. On other platforms, the flag is ignored, and the user is responsible for managing the lifetime of the child processes. If the parent Ray worker process dies, the child processes will continue to run, as if the flag is always disabled.

If user uses Linux < 3.4, the flag is not present and the Raylet crashes. The user can upgrade the kernel to use this feature, or disable the feature by setting the environment variable ``RAY_kill_child_processes_on_worker_exit`` to ``false``.


⚠️ CAVEAT: daemon user processes may be killed
---------------------------------------------

Note that, if a user process exits, all its subprocess are killed immediately. For example if user spawns a "launcher" process which spawns a "daemon" process, and then the launcher exits, the daemon is killed immediately, even if the **Ray task or actor is still alive**.

.. code-block:: python

  import ray
  import time
  import subprocess

  @ray.remote
  def spawn_daemon():
    # Start the bash script that spawns the daemon process
    subprocess.Popen(["bash", "-c", "nohup ./daemon &"])
    # sleep loop
    while True:
        time.sleep(1000)

  # Spawn the bash script that spawns the daemon process
  task = spawn_daemon.remote()

  # Infinite wait
  ray.get(task)

In the example above, user spawned a bash process which spawned a daemon process. After that, the bash process exits.

.. code-block::

  raylet -> core worker (the task) -> bash (user, exits) -> daemon (user, killed immediately)


With the feature enabled, after the bash exits, the daemon is **killed immediately**, even though the task itself is still running.

If you want to start a daemon like that, consider:

1. disabling this feature.
2. keep the launcher process alive.

Under the hood
-------------------------

This feature is implemented by setting the `prctl(PR_SET_CHILD_SUBREAPER, SIGKILL)` flag on the Raylet process which spawns all Ray workers. See [prctl(2)](https://man7.org/linux/man-pages/man2/prctl.2.html). This flag makes the Raylet process a "subreaper" which means that if a descendant child process dies, the dead child's children processes reparent to the Raylet process.

Raylet maintains a list of "known" direct children pid it spawns, and when the Raylet process receives the SIGCHLD signal, it knows that one of its child processes (e.g. core workers) has died, and maybe there are reparented orphan processes. Raylet lists all children pids (with ppid = raylet pid), and if a child pid is not "known" (i.e. not in the list of direct children pids), Raylet thinks it is an orphan process and kills it via `SIGKILL`.

For a deep chain of process creations, Raylet would do the killing step by step. For example, in a chain like this:

.. code-block::

  raylet -> core worker -> user process A -> user process B -> user process C
 
When the core worker dies, Raylet kills the user process A, because it's not on the "known" children list. When user process A dies, Raylet kills user process B, and so on.

Related PR: `Use subreaper to kill unowned subprocesses in raylet. (#42992) <https://github.com/ray-project/ray/pull/42992>`_