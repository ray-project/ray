---
myst:
  html_meta:
    description: "Pattern: use an async actor so its methods run concurrently on one worker, overlapping I/O-bound operations."
---

# Pattern: Using asyncio to run actor methods concurrently

By default, a Ray {ref}`actor <ray-remote-classes>` runs in a single thread and executes method calls sequentially, so a long-running method call blocks all the calls that follow it. In this pattern, you use `await` to yield control from the long-running method call so other method calls can run concurrently. A method normally yields control when it's doing I/O operations, but you can also use `await asyncio.sleep(0)` to yield control explicitly.

:::{note}
You can also use {ref}`threaded actors <threaded-actors>` to achieve concurrency.
:::

## Example use case

You have an actor with a long polling method that continuously fetches tasks from the remote store and executes them. You also want to query the number of tasks executed while the long polling method is running.

With the default actor, the code looks like the following:

```{literalinclude} ../doc_code/pattern_async_actor.py
:language: python
:start-after: __sync_actor_start__
:end-before: __sync_actor_end__
```

This code is a problem because the `TaskExecutor.run` method runs forever and never yields control to run other methods. To solve this problem, use {ref}`async actors <async-actors>` and `await` to yield control:

```{literalinclude} ../doc_code/pattern_async_actor.py
:language: python
:start-after: __async_actor_start__
:end-before: __async_actor_end__
```

Here, instead of using the blocking {func}`ray.get() <ray.get>` to get the value of an object ref, the method uses `await` so it can yield control while it waits for the object to be fetched.
