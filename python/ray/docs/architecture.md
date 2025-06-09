## Limitations of Default Serial Execution on Actors

Currently, tasks submitted to actors are executed serially by default (my actor is not an asynchronous actor and I do not wish for concurrency). In extreme scenarios, this can lead to resource wastage and a noticeable decrease in job throughput. For example, consider a task sequence: `read_parquet -> map_batches A -> map_batches B -> write_parquet`:

1. `map_batches A` completes some tasks and outputs objects a1, a2, a3, and a4.
2. Actor tasks are submitted to actor B, corresponding to tasks b1, b2, b3, and b4.
3. The worker handling object a1 crashes, causing the object to be lost. However, due to insufficient resources, actor A remains in a restarting state and cannot recover.
4. The head task b1 in actor B's queue cannot be scheduled due to the lost dependency, causing this actor to remain in a waiting state while occupying valuable GPU resources.

In my scenario, I primarily use elastic resources characterized by frequent preemption and significant fluctuations in available resources, with rapid increases and decreases. During this process, many actors are found to be in a restarting state, leading to job throughput dropping to nearly zero.

Therefore, I hope to allow Ray data actor tasks to execute out of order. 