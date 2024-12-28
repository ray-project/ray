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
  + If a task / actor is not annotated with resource usage, ray caps max resource usage via heuristric estimation (i.e. 80% of the total logical resource). This is implemented by setting a max value on `/sys/fs/cgroup/ray_node_<node_id>/application` node (see chart below).

TODO(hjiang): reserve minimum resource will be supported in the future.

### Prerequisites

- The feature is built upon cgroup, which only supports linux;
- Only cgroup v2 is supported, meanwhile ray also requires raylet process to have write permission and cgroup v2 be mounted in rw mode;
- If any of the prerequisites unsatisfied, when physical mode enabled, ray logs error with program keep working.

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
                .../default/  .../<task_id>_<attempt_id> (*N)
```

- Raylet is responsible to create cgroup folder at startup, and cleanup the folder at its destruction
- Each ray node having their own cgroup folder, which contains the node id to differentiate with other raylet(s)
- `/sys/fs/cgroup/ray_node_<node_id>/application` is where ray sets overall max resource for all application processes
- If a task / actor execute with their max resource specified, they will be placed in a dedicated cgroup, identified by the task id and attempt id
- Otherwise they will be placed under default application cgroup, having their max consumption bound by `/sys/fs/cgroup/ray_node_<node_id>/application`

#### Cgroup lifecycle

A cgroup's lifecycle is bound by a task / actor attempt.
Before execution, the worker PID is placed into the cgroup;
after its completion, the idle worker is put back to worker pool and reused later, with its PID moved back to the default cgroup, and cgroup destructed if any.

TODO(hjiang): Add discussion on how to deal with situations when task finishes, while some of the processes don't finish.
