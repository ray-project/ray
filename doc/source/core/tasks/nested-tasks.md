---
myst:
  html_meta:
    description: "How to call remote functions from other remote functions, and how nested tasks yield their resources while blocked."
---

# Nested remote functions

A remote function can call other remote functions, which results in nested tasks. Consider the following example:

```{literalinclude} ../doc_code/nested-tasks.py
:language: python
:start-after: __nested_start__
:end-before: __nested_end__
```

Calling `g` and `h` produces the following output:

```bash
>>> ray.get(g.remote())
[ObjectRef(b1457ba0911ae84989aae86f89409e953dd9a80e),
 ObjectRef(7c14a1d13a56d8dc01e800761a66f09201104275),
 ObjectRef(99763728ffc1a2c0766a2000ebabded52514e9a6),
 ObjectRef(9c2f372e1933b04b2936bb6f58161285829b9914)]

>>> ray.get(h.remote())
[1, 1, 1, 1]

```

:::{note}
Define `f` before you define `g` and `h`. As soon as you define `g`, Ray pickles it and ships it to the workers. If you haven't defined `f` yet, the definition of `g` is incomplete.
:::

## Yielding resources while blocked

Ray releases a task's CPU resources while the task blocks. This prevents deadlocks where nested tasks wait for CPU resources that the parent task holds. Consider the following remote function:

```{literalinclude} ../doc_code/nested-tasks.py
:language: python
:start-after: __yield_start__
:end-before: __yield_end__
```

While a `g` task runs, it releases its CPU resources when it blocks in the call to `ray.get`, and it reacquires them when `ray.get` returns. The task keeps its GPU resources for its whole lifetime because it most likely continues to use GPU memory.
