---
myst:
  html_meta:
    description: "Pattern: call remote functions from inside remote functions to express nested parallelism such as divide-and-conquer."
---

(nested-tasks)=

# Pattern: Using nested tasks to achieve nested parallelism

In this pattern, a remote task can dynamically call other remote tasks, including itself, to achieve nested parallelism. This pattern is useful when you can parallelize the sub-tasks.

Nested tasks come with their own costs, such as extra worker processes, scheduling overhead, and bookkeeping overhead. To get a speedup from nested parallelism, make sure each of your nested tasks does significant work. See {doc}`too-fine-grained-tasks` for more details.

## Example use case

You want to quick-sort a large list of numbers. With nested tasks, you can sort the list in a distributed and parallel fashion.

```{figure} ../images/tree-of-tasks.svg
Tree of tasks
```

## Code example

```{literalinclude} ../doc_code/pattern_nested_tasks.py
:language: python
:start-after: __pattern_start__
:end-before: __pattern_end__
```

The example calls {func}`ray.get() <ray.get>` after both `quick_sort_distributed` function invocations take place. This ordering maximizes parallelism in the workload. See {doc}`ray-get-loop` for more details.

Notice in the preceding execution times that the non-distributed version is faster for smaller tasks. However, as task execution time increases because the lists to sort are larger, the distributed version is faster.
