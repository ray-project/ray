## Ray core cgroup documentation

### Physical execution mode

Ray core supports a physical execution mode, which allows users to cap resource consumption for their applications.

A few benefits:
- It prevents application from eating up unlimited resource to starve other applications running on the same node;
- If physical execution mode is enabled, Ray uses cgroup to restrict resource usage. Now we only support using `memory` as cgroup `memory.max` to cap a task process (and all its subprocesses recursively)'s max memory usage. For example,
```python
@ray.remote(memory=500 * 1024 * 1024)
def some_function(x):
    pass

obj = some_function.remote()
```
This function is limited by 500MiB memory usage, and if it tries to use more, it OOMs and fails.
  + If a task / actor is not annotated with resource usage, ray caps max resource usage via heuristric estimation.

TODO(hjiang): reserve minimum resource will be supported in the future.

### Prerequisites

- The feature is built upon cgroup, which only supports linux;
- Only cgroup v2 is supported, meanwhile ray also requires application to have write permission and cgroup v2 be mounted in rw mode;
- If any of the prerequisites unsatisfied, when physical mode enabled, ray logs error with program keep working.

### Disclaimer

- At the initial version, ray caps max resource usage via heuristric estimation (TODO: support user passed-in value).

### Implementation details

#### Cgroup hierarchy

cgroup v2 folders are created in tree structure as follows

```
        /sys/fs/cgroup/ray_node_<node_id>
        /                          \
.../system_cgroup           .../application_cgroup
                            /                   \
                .../default_cgroup/  .../<task_id>_<attempt_id>_cgroup (*N)
```

- Raylet is responsible to create and cleanup its own cgroup folder
- Each ray node having their own cgroup folder, which contains the node id to differentiate with other raylet(s)
- `/sys/fs/cgroup/ray_node_<node_id>/application_cgroup` is where ray sets overall max resource for all application processes
- If a task / actor execute with their max resource specified, they will be placed in a dedicated cgroup, identified by the task id and attempt id
- Otherwise they will be placed under default application cgroup, having their max consumption bound by `/sys/fs/cgroup/ray_node_<node_id>/application_cgroup`

#### Cgroup lifecycle

A cgroup's lifecycle is bound by a task / actor attempt.
Before execution, the worker PID is placed into the cgroup;
after its completion, the idle worker is put back to worker pool and reused later, with its PID moved back to the default cgroup, and cgroup destructed if any.
