## Ray core cgroup documentation

### Physical execution mode

Ray core supports a physical execution mode, which allows users to cap resource consumption for their applications.

A few benefits:
- If physical execution mode is enabled, Ray uses cgroup to restrict resource usage, so other processes running on the same machine (i.e. system processes like raylet and GCS) won't get starved or even killed. Now we only support using `memory` as cgroup `memory.max` to cap a task process (and all its subprocesses recursively)'s max memory usage. For example,
```python
@ray.remote(memory=500 * 1024 * 1024)
def some_function(x):
    pass

obj = some_function.remote()
```
This function is limited by 500MiB memory usage, and if it tries to use more, it OOMs and fails.
  + User can set the limit to any number at node start; if not, ray will take a heuristric estimation on all application processes (i.e. 80% of the total logical resource). This is implemented by setting a max value on `/sys/fs/cgroup/ray_node_<node_id>/application` node (see chart below).

TODO(hjiang): reserve minimum resource will be supported in the future.

### Prerequisites

- Ray runs in a Linux environment that supports Cgroup V2.
- The cgroup2 filesystem is mounted at `/sys/fs/cgroup`.
- Raylet has write permission to that mounted directory.
- If any of the prerequisites unsatisfied, when physical mode enabled, ray logs error and continue running.

### Disclaimer

- At the initial version, ray caps max resource usage via heuristric estimation (TODO: support user passed-in value).

### Implementation details

#### Cgroup hierarchy

cgroup v2 folders are created in tree structure as follows

```
     /sys/fs/cgroup/ray_node_<node_id>
        /                       \
.../internal           .../application
                        /            \
                .../default  .../<task_id>_<attempt_id> (*N)
```

- Each ray node having their own cgroup folder, which contains the node id to differentiate with other raylet(s); in detail, raylet is responsible to create cgroup folder `/sys/fs/cgroup/ray_node_<node_id>`, `/sys/fs/cgroup/ray_node_<node_id>/internal` and `/sys/fs/cgroup/ray_node_<node_id>/application` at startup, and cleans up the folder upon process exit;
- `/sys/fs/cgroup/ray_node_<node_id>/application` is where ray sets overall max resource for all application processes
  + The max resource respects users' input on node start, or a heuristic value 80% of all logical resource will be taken
- If a task / actor execute with their max resource specified, they will be placed in a dedicated cgroup, identified by the task id and attempt id; the cgroup path is `/sys/fs/cgroup/ray_node_<node_id>/application/<task_id>_<attempt_id>`
  + Task id is a string which uniquely identifies a task
  + Attempt id is a monotonically increasing integer, which is used to different executions for the same task and indicates their order
- Otherwise they will be placed under default application cgroup, having their max consumption bound by `/sys/fs/cgroup/ray_node_<node_id>/application`

TODO(hjiang): Add more details on attempt id. For example, whether it's raylet-wise or task-wise.

#### Cgroup lifecycle

A cgroup's lifecycle is bound by a task / actor attempt.
Before execution, the worker PID is placed into the cgroup;
after its completion, the idle worker is put back to worker pool and reused later, with its PID moved back to the default cgroup, and cgroup destructed if any.

TODO(hjiang): Add discussion on how to deal with situations when task finishes, while some of the processes don't finish.
