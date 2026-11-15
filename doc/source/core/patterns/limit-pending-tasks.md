---
myst:
  html_meta:
    description: "Pattern: use ray.wait to bound the number of in-flight tasks so the submission loop doesn't outrun the cluster."
---

(core-patterns-limit-pending-tasks)=

# Pattern: Using ray.wait to limit the number of pending tasks

In this pattern, you use {func}`ray.wait() <ray.wait>` to limit the number of pending tasks.

If you continuously submit tasks faster than they finish processing, tasks accumulate in the pending task queue, which can eventually cause out-of-memory (OOM) errors. Use `ray.wait()` to apply backpressure and limit the number of pending tasks, so the pending task queue doesn't grow indefinitely and cause OOM.

:::{note}
If you submit a finite number of tasks, you're unlikely to hit this issue, because each task uses only a small amount of memory for bookkeeping in the queue. The issue is more likely when you have an infinite stream of tasks to run.
:::

:::{note}
Use this method primarily to limit how many tasks are in flight at the same time. You can also use it to limit how many tasks run *concurrently*, but avoid doing so, because it can hurt scheduling performance. Ray decides task parallelism automatically based on resource availability, so to adjust how many tasks can run concurrently, {ref}`modify each task's resource requirements <core-patterns-limit-running-tasks>` instead.
:::

## Example use case

You have a worker actor that processes tasks at a rate of X tasks per second, and you want to submit tasks to it at a rate lower than X to avoid OOM.

For example, Ray Serve uses this pattern to limit the number of pending queries for each worker.

```{figure} ../images/limit-pending-tasks.svg
Limit the number of pending tasks
```

## Code example

**Without backpressure:**

```{literalinclude} ../doc_code/limit_pending_tasks.py
:language: python
:start-after: __without_backpressure_start__
:end-before: __without_backpressure_end__
```

**With backpressure:**

```{literalinclude} ../doc_code/limit_pending_tasks.py
:language: python
:start-after: __with_backpressure_start__
:end-before: __with_backpressure_end__
```
