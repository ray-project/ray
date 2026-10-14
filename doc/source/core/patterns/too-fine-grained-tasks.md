---
myst:
  html_meta:
    description: "Anti-pattern: splitting work into very small tasks lets per-task overhead dominate; batch work into coarser tasks."
---

# Anti-pattern: Over-parallelizing with too fine-grained tasks harms speedup

Avoid over-parallelizing. Parallelizing or distributing tasks usually has higher overhead than an ordinary function call. If you parallelize a function that runs quickly, the overhead could take longer than the function call itself.

To handle this problem, avoid parallelizing too much. If a function or task is too small, use a technique called **batching** to make each task do more meaningful work in a single call.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_too_fine_grained_tasks.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach:** Use batching.

```{literalinclude} ../doc_code/anti_pattern_too_fine_grained_tasks.py
:language: python
:start-after: __batching_start__
:end-before: __batching_end__
```

The preceding example shows that over-parallelizing has higher overhead, and the program runs slower than the serial version. With batching and a proper batch size, you can amortize the overhead and achieve the expected speedup.
