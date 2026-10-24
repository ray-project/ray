---
myst:
  html_meta:
    description: "Choose between single-controller and single-program multiple-data programming models with Ray tasks, actors, and objects."
---

(programming-models)=

# Choose a programming model

Ray's tasks, actors, and objects support different ways to organize a distributed application. Two common shapes are a single controller and single-program, multiple-data (SPMD) execution. You can also combine both shapes in one application.

This page describes the shape of an application. It doesn't prescribe a programming model for a particular Ray library or workload.

## What is a single-controller application?

In a single-controller application, one process coordinates the work. In a Ray job, the driver process can act as the controller. It submits tasks, creates actors, passes actor handles to other tasks or actors, and collects results through object references.

The controller can assign different responsibilities to different workers. For example, one actor can maintain state while tasks process inputs and return results. The controller can also make the next scheduling decision after it receives a result.

Use this shape when the application needs a coordinating process to sequence stages, assign roles, or make decisions based on intermediate results. See the {ref}`Tasks <ray-remote-functions>`, {ref}`Actors <actor-guide>`, and {ref}`Objects <objects-in-ray>` user guides for the primitives that express these relationships.

For example, the driver can coordinate a sequence of tasks by passing each result to the next stage:

```python
import ray


@ray.remote
def load_batch():
    return [1, 2, 3]


@ray.remote
def summarize(batch):
    return sum(batch)


batch_ref = load_batch.remote()
summary_ref = summarize.remote(batch_ref)
print(ray.get(summary_ref))
```

The driver controls the order of the stages through the object references, so this is a single-controller application.

## What is SPMD execution?

In SPMD execution, each worker runs the same program or worker function on a different part of the input. The workers follow the same control flow while their data partitions differ. Ray tasks can run the same remote function for multiple inputs, and a group of actors can run the same method for different partitions.

Use this shape when you can split the input into partitions and apply the same computation to each partition. The driver can create the tasks or actors and collect their object references, while the workers perform the repeated computation.

For example, the driver can submit the same task for each input partition and collect the results:

```python
import ray


@ray.remote
def sum_partition(partition):
    return sum(partition)


partitions = [[1, 2], [3, 4], [5, 6]]
result_refs = [sum_partition.remote(partition) for partition in partitions]
print(sum(ray.get(result_refs)))
```

Each worker runs the same `sum_partition` function, but receives a different partition of the input.

## How do the shapes compare?

| Aspect | Single controller | SPMD |
| --- | --- | --- |
| Control flow | One controller coordinates worker actions. | Workers follow the same program or worker function. |
| Worker roles | Workers can have different responsibilities. | Workers perform the same kind of computation on different data. |
| Ray primitives | The controller uses task calls, actor handles, and object references to coordinate work. | The driver submits the same task or actor method for multiple input partitions. |
| A useful starting point | Model the coordinator first, then add workers for each role. | Define the worker function, then decide how to partition the input. |

## Can you combine both shapes?

Yes. A driver can create a group of actors, assign each actor a different input partition, and invoke the same method on every actor. The driver still coordinates the group, so the application combines a single controller with SPMD workers.

Choose the boundary between the controller and the workers based on the responsibilities in your application. Keep coordination in the driver when it depends on global state or intermediate results. Move repeated, partitioned computation into tasks or actors when each worker can make progress from its own input.

For examples of composing tasks, actors, and object references, see the {ref}`Ray Core walkthrough <core-walkthrough>` and {ref}`Ray Core design patterns <core-patterns>`.
