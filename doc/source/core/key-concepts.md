---
myst:
  html_meta:
    description: "Ray Core primitives: tasks (remote functions), actors (stateful remote classes), objects in the distributed object store, and placement groups."
---

(core-key-concepts)=

# Key concepts

This page describes Ray's key concepts. These primitives work together to support a broad range of distributed applications.

To choose how your application coordinates work, see {ref}`Choose a programming model <programming-models>`.

(task-key-concept)=

## Tasks

Ray runs arbitrary functions asynchronously on separate worker processes. These asynchronous Ray functions are *tasks*. You can specify a task's resource requirements in terms of CPUs, GPUs, and custom resources. The cluster scheduler uses these resource requests to distribute tasks across the cluster for parallel execution.

See the {ref}`user guide for tasks <ray-remote-functions>`.

(actor-key-concept)=

## Actors

Actors extend the Ray API from functions, which are tasks, to classes. An actor is a stateful worker, which you can also think of as a service. When you instantiate an actor, Ray creates a worker for it and schedules the actor's methods on that specific worker. The methods can access and mutate the state of that worker. Like tasks, actors support CPU, GPU, and custom resource requirements.

See the {ref}`user guide for actors <actor-guide>`.

## Objects

Tasks and actors create objects and compute on them. These objects are *remote objects* because Ray can store them anywhere in a Ray cluster. You use *object refs* to refer to them. Ray caches remote objects in its distributed [shared-memory](https://en.wikipedia.org/wiki/Shared_memory) *object store*, with one object store per node in the cluster. A remote object can live on one or more nodes, independent of who holds the object ref.

See the {ref}`user guide for objects <objects-in-ray>`.

## Placement groups

Use placement groups to atomically reserve groups of resources across multiple nodes. With a placement group, you can schedule tasks and actors packed as close as possible for locality with the `PACK` strategy, or spread them apart with the `SPREAD` strategy. A common use case is gang-scheduling actors or tasks.

See the {ref}`user guide for placement groups <ray-placement-group-doc-ref>`.

## Environment dependencies

When Ray executes tasks and actors on remote machines, their environment dependencies, such as Python packages, local files, and environment variables, must be available on those machines. To make them available, do one of the following:

1. Prepare your dependencies on the cluster in advance with the Ray {ref}`cluster launcher <vm-cluster-quick-start>`.
1. Use Ray's {ref}`runtime environments <runtime-environments>` to install them on the fly.

See the {ref}`user guide for environment dependencies <handling_dependencies>`.
