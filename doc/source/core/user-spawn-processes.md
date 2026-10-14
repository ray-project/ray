---
myst:
  html_meta:
    description: "Lifetime of processes your code spawns inside Ray workers, including killing them on worker exit and zombie reaping behavior."
---

# Lifetimes of user-spawned processes

When you spawn child processes from Ray workers, you're responsible for managing the lifetime of those child processes. That isn't always possible, especially when a worker crashes or when a library, such as the PyTorch data loader, spawns the child processes.

To avoid leaking user-spawned processes, Ray provides mechanisms to kill all user-spawned processes when the worker that started them exits. This feature prevents GPU memory leaks from child processes, such as the ones PyTorch spawns.

Ray provides the following three mechanisms to kill child processes on worker exit:

- `RAY_kill_child_processes_on_worker_exit`: Defaults to `true` and works only on Linux. When `true`, the worker kills all of its *direct* child processes on exit. This mechanism doesn't work if the worker crashes. It isn't recursive, so it doesn't kill grandchild processes.

- `RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper`: Defaults to `false` and works only on Linux 3.4 and later. When `true`, the raylet *recursively* kills any child and grandchild processes that the worker spawned, after the worker exits. This mechanism works even if the worker crashes. The raylet kills these processes within 10 seconds of the worker's death.

- `RAY_process_group_cleanup_enabled`: Defaults to `true` and works on POSIX platforms. When `true`, Ray isolates each worker in its own process group at spawn and cleans up the worker's process group through `killpg` when the worker exits. Processes that intentionally call `setsid()` detach from the group, so this cleanup doesn't kill them. This is the preferred mechanism, and it supersedes the deprecated subreaper-based cleanup.

The subreaper isn't available on non-Linux platforms. Per-worker process groups work on POSIX platforms. On Windows, neither the subreaper nor process groups apply. On platforms without support, manage child processes explicitly.

:::{note}
The feature is a last resort to kill orphaned processes, not a replacement for proper process management. Manage the lifetime of your processes and clean them up properly.
:::

```{contents}
:local:
```

## User-spawned process killed on worker exit

The following example enables the raylet subreaper and uses a Ray actor to spawn a user process that runs `sleep`.

```{testcode}
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
```


## Enable the subreaper feature

The subreaper feature is deprecated. Use `process_group_cleanup_enabled` instead, which is on by default. To enable the subreaper feature anyway, set it when you start the cluster, through `_system_config` or the equivalent cluster configuration. You must restart the cluster to apply the change. For example, set the environment variable when you start the head node:

```bash
RAY_kill_child_processes_on_worker_exit_with_raylet_subreaper=true ray start --head
```

Alternatively, pass a `_system_config` to `ray.init()`:

```
ray.init(_system_config={"kill_child_processes_on_worker_exit_with_raylet_subreaper":True})
```


## Caution: The core worker reaps zombie processes

When you enable the subreaper, the worker process also becomes a subreaper on Linux, which means some grandchild processes can be reparented to the worker process. The worker sets `SIGCHLD` to `SIG_IGN`. To wait for a child process to exit, for example with `waitpid`, reset `SIGCHLD` to `SIG_DFL` first:

```
import signal
signal.signal(signal.SIGCHLD, signal.SIG_DFL)
```


## How does the subreaper work?

Ray sets the `prctl(PR_SET_CHILD_SUBREAPER, 1)` flag on the raylet process, which spawns all Ray workers. See [prctl(2)](https://man7.org/linux/man-pages/man2/prctl.2.html). This flag makes the raylet process a "subreaper." If a descendant process dies, the dead process's children reparent to the raylet process. The subreaper is deprecated in favor of per-worker process groups.

The raylet keeps a list of the "known" direct child PIDs that it spawns. When the raylet process receives a `SIGCHLD` signal, it knows that one of its child processes, such as a worker, has died, and that reparented orphan processes might exist. The raylet then lists all of its child processes, the ones whose parent process ID (PPID) is the raylet PID. If a child PID isn't in the list of known direct children, the raylet treats that process as an orphan and kills it with `SIGKILL`.

For a deep chain of processes, the raylet kills them one step at a time. Consider the following chain:

```
raylet -> the worker -> user process A -> user process B -> user process C
```

When the worker dies, the raylet kills `user process A`, because it isn't on the "known" children list. When `user process A` dies, the raylet kills `user process B`, and so on.

In one edge case, the worker is still alive but `user process A` is dead, so `user process B` gets reparented and risks being killed. To mitigate this, Ray also sets the worker as a subreaper, so it can adopt the reparented processes. The core worker doesn't kill unknown child processes, so a user "daemon" process such as `user process B` that outlives `user process A` can keep running. However, if the worker dies, the user daemon process gets reparented to the raylet, which kills it.

Related PR: [Use subreaper to kill unowned subprocesses in raylet. (#42992)](https://github.com/ray-project/ray/pull/42992)
