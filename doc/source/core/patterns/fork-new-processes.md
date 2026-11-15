---
myst:
  html_meta:
    description: "Anti-pattern: forking processes inside a Ray task or actor breaks Ray's process management and resource accounting."
---

(forking-ray-processes-antipattern)=

# Anti-pattern: Forking new processes in application code

Don't fork new processes in Ray application code, such as the driver, tasks, or actors. Instead, use the "spawn" method to start new processes, or use Ray tasks and actors to parallelize your workload.

Ray manages the lifecycle of processes for you. Ray objects, tasks, and actors manage sockets to communicate with the raylet and the GCS. If you fork new processes in your application code, the forked processes could share the same sockets without any synchronization, which can lead to corrupted messages and unexpected behavior.

To avoid these problems, use one of the following two approaches:

- Use the "spawn" method to start new processes, so that the parent process's memory space isn't copied to the child processes.
- Use Ray tasks and actors to parallelize your workload, and let Ray manage the lifecycle of the processes for you.

## Code example

```{literalinclude} ../doc_code/anti_pattern_fork_new_processes.py
:language: python
```
