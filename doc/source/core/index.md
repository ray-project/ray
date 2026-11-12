---
myst:
  html_meta:
    description: "An introduction to tasks, actors, and objects, the distributed computing primitives of Ray, with examples that turn Python functions and classes into distributed apps."
---

(core-walkthrough)=

# What's Ray Core?

```{toctree}
:maxdepth: 1
:hidden:

Key Concepts <key-concepts>
User Guides <user-guide>
Examples <examples/index>
Internals <internals/index>
```

Ray Core is a distributed computing framework for building and scaling distributed applications. It provides three essential primitives: tasks, actors, and objects. This walkthrough introduces each one with examples that show how to turn your Python functions and classes into Ray tasks and actors, and how to work with Ray objects.

:::{note}
Ray offers an experimental API that transfers objects over Gloo, NCCL, NIXL, or your own transport, as an alternative to the default object store, which uses shared memory and gRPC. For details, see {ref}`Ray Direct Transport <direct-transport>`.
:::

## Getting started

To get started, install Ray with `pip install -U ray`. For other installation options, see {ref}`Installing Ray <installation>`.

Start by importing and initializing Ray:

```{literalinclude} doc_code/getting_started.py
:language: python
:start-after: __starting_ray_start__
:end-before: __starting_ray_end__
```

:::{note}
If you don't call `ray.init()` explicitly, the first Ray remote API call implicitly calls `ray.init()` with no arguments.
:::

## Running a task

Tasks are the simplest way to parallelize your Python functions across a Ray cluster. To create and run a task, do the following:

1. Decorate your function with `@ray.remote` to mark it to run remotely.
1. Call the function with `.remote()` instead of a normal function call.
1. Use `ray.get()` to retrieve the result from the returned future, which Ray calls an *object ref*.

The following example creates and runs a task:

```{literalinclude} doc_code/getting_started.py
:language: python
:start-after: __running_task_start__
:end-before: __running_task_end__
```

## Calling an actor

Tasks are stateless. Ray actors are stateful workers that maintain their internal state between method calls. When you instantiate an actor, the following happens:

- Ray starts a dedicated worker process somewhere in your cluster.
- The actor's methods run on that specific worker and can access and modify its state.
- The actor executes method calls serially in the order it receives them, which preserves consistency.

The following example defines and calls a `Counter` actor:

```{literalinclude} doc_code/getting_started.py
:language: python
:start-after: __calling_actor_start__
:end-before: __calling_actor_end__
```

The preceding example shows basic actor usage. For a fuller example that combines tasks and actors, see the {ref}`Monte Carlo Pi estimation example <monte-carlo-pi>`.

## Passing objects

Ray's distributed object store manages data across your cluster. You work with objects in Ray in three main ways:

- **Implicit creation**: When tasks and actors return values, Ray automatically stores them in its {ref}`distributed object store <objects-in-ray>` and returns object refs that you can retrieve later.
- **Explicit creation**: Use `ray.put()` to place objects in the store directly.
- **Passing references**: Pass object refs to other tasks and actors, which avoids unnecessary data copying and supports lazy execution.

The following example shows each technique:

```{literalinclude} doc_code/getting_started.py
:language: python
:start-after: __passing_object_start__
:end-before: __passing_object_end__
```

## Next steps

:::{tip}
To monitor your application's performance and resource usage, see the {ref}`Ray dashboard <observability-getting-started>`.
:::

You can combine Ray's primitives to express virtually any distributed computation pattern. To learn more about Ray's {ref}`key concepts <core-key-concepts>`, see the following user guides:

::::{grid} 1 2 3 3
:gutter: 1
:class-container: container pb-3

:::{grid-item-card}
:img-top: /images/tasks.png
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} ray-remote-functions

Using remote functions as tasks
```
:::

:::{grid-item-card}
:img-top: /images/actors.png
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} ray-remote-classes

Using remote classes as actors
```
:::

:::{grid-item-card}
:img-top: /images/objects.png
:class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

```{button-ref} objects-in-ray

Working with Ray objects
```
:::
::::
