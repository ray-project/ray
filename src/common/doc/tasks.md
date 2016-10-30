# Task specifications, task instances and task logs

A *task specification* contains all information that is needed for computing
the results of a task:

- The ID of the task
- The function ID of the function that executes the task
- The arguments (either object IDs for pass by reference
or values for pass by value)
- The IDs of the result objects

From these, a task ID can be computed which is also stored in the task
specification.

A *task* represents the execution of a task specification.
It consists of:

- A scheduling state (WAITING, SCHEDULED, RUNNING, DONE)
- The target node where the task is scheduled or executed
- The task specification

The task data structures are defined in `common/task.h`.

The *task table* is a mapping from the task ID to the *task* information. It is
updated by various parts of the system:

1. The local scheduler writes it with status WAITING when submits a task to the global scheduler
2. The global scheduler appends an update WAITING -> SCHEDULED together with the node ID when assigning the task to a local scheduler
3. The local scheduler appends an update SCHEDULED -> RUNNING when it assigns a task to a worker
4. The local scheduler appends an update RUNNING -> DONE when the task finishes execution

The task table is defined in `common/state/task_table.h`.
