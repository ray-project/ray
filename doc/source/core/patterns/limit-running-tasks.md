---
myst:
  html_meta:
    description: "Pattern: use custom resource requirements to cap how many tasks or actors run concurrently, limiting memory pressure."
---

(core-patterns-limit-running-tasks)=

# Pattern: Using resources to limit the number of concurrently running tasks

In this pattern, you use {ref}`resources <resource-requirements>` to limit the number of concurrently running tasks.

By default, Ray tasks require 1 CPU each and Ray actors require 0 CPU each, so the scheduler limits task concurrency to the available CPUs and doesn't limit actor concurrency. Some tasks use more than 1 CPU, for example through multithreading. These tasks may slow down because of interference from concurrent tasks, but are otherwise safe to run.

However, tasks or actors that use more than their proportionate share of memory may overload a node and cause issues such as out-of-memory (OOM) errors. If so, you can reduce the number of concurrently running tasks or actors on each node by increasing the amount of resources they request. This works because Ray ensures that the sum of the resource requirements of all concurrently running tasks and actors on a node doesn't exceed the node's total resources.

:::{note}
For actor tasks, the number of running actors limits the number of actor tasks that can run concurrently.
:::

## Example use case

You have a data processing workload that processes each input file independently using Ray {ref}`remote functions <ray-remote-functions>`. Because each task loads the input data into heap memory and processes it, running too many tasks at once can cause OOM errors. In this case, use the `memory` resource to limit the number of concurrently running tasks. Other resources, such as `num_cpus`, can achieve the same goal. Like `num_cpus`, the `memory` resource requirement is *logical*, meaning that Ray doesn't enforce the physical memory usage of each task if it exceeds this amount.

## Code example

**Without limit:**

```{literalinclude} ../doc_code/limit_running_tasks.py
:language: python
:start-after: __without_limit_start__
:end-before: __without_limit_end__
```

**With limit:**

```{literalinclude} ../doc_code/limit_running_tasks.py
:language: python
:start-after: __with_limit_start__
:end-before: __with_limit_end__
```
