# Scheduler

The scheduling strategies currently implemented in Halo are fairly basic and
all use a central scheduler.

* The naive scheduler assigns tasks to workers just taking into account
dependencies between tasks (no other information like data locality). It is
supposed to be an example for how to write a scheduler. We do not recommend
its use and it only works well for single node setups. Tasks are assigned in the
following way: For each idle worker, we iterate over the tasks in the task
queue. The first task that has all its requirements satisfied will be scheduled
on the worker.

* The locality aware scheduler is more suited for multi node setups, but still
inappropriate for very large clusters. This is because the computational
overhead for each scheduling decision is O(mn) where m is the number of idle
workers and n is the number of tasks in the task queue. For each idle worker,
all tasks in the task queue are considered and the one that requires the
smallest number of objects to be shipped will be executed.

We expect to implement more refined scheduling strategies in the future,
including more computationally efficient location aware scheduling,
scheduling that takes into account sizes of the shipped objects, and strategies
that do not require a central scheduler (which is a bottleneck for large
clusters).
