---
myst:
  html_meta:
    description: "Utility classes for actors: ActorPool for load-balanced method calls and Ray Queue for message passing between tasks and actors."
---

# Utility classes

This page describes `ActorPool`, which load-balances method calls across a pool of actors, and Ray `Queue`, which passes messages between tasks and actors.

## Actor pool

::::{tab-set}
:::{tab-item} Python
The `ray.util` module contains `ActorPool`, a utility class similar to `multiprocessing.Pool`. Use it to schedule Ray tasks over a fixed pool of actors.

```{literalinclude} ../doc_code/actor-pool.py
```

See the {class}`package reference <ray.util.ActorPool>` for more information.
:::

:::{tab-item} Java
Actor pool isn't available in Java yet.
:::

:::{tab-item} C++
Actor pool isn't available in C++ yet.
:::
::::

## Message passing using Ray Queue

One signal isn't always enough for synchronization. To send data among many tasks or actors, use {class}`ray.util.queue.Queue <ray.util.queue.Queue>`.

```{literalinclude} ../doc_code/actor-queue.py
```

The Ray `Queue` API is similar to Python's `asyncio.Queue` and `queue.Queue`.
