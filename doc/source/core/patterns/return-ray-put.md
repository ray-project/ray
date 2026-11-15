---
myst:
  html_meta:
    description: "Anti-pattern: returning ray.put ObjectRefs from a task adds a copy and weakens fault tolerance; return the value instead."
---

# Anti-pattern: Returning ray.put() ObjectRefs from a task harms performance and fault tolerance

Avoid calling {func}`ray.put() <ray.put>` on task return values and returning the resulting object refs. Instead, return the values directly if possible.

Returning `ray.put()` object refs from a task is an anti-pattern for the following reasons:

- It prevents Ray from inlining small return values. As a performance optimization, Ray returns small values of 100 KB or less inline, directly to the caller, without going through the distributed object store. `ray.put()` unconditionally stores the value in the object store, which makes this optimization impossible.
- Returning object refs involves an extra distributed reference counting protocol, which is slower than returning the values directly.
- It's less {ref}`fault tolerant <fault-tolerance>`. The worker process that calls `ray.put()` is the "owner" of the returned `ObjectRef`, and the return value fate-shares with the owner. If the worker process dies, the return value is lost. In contrast, when the task returns the value directly, the caller process owns the return value. The caller is often the driver.

## Code example

To return a single value, whether it's small or large, return it directly.

```{literalinclude} ../doc_code/anti_pattern_return_ray_put.py
:language: python
:start-after: __return_single_value_start__
:end-before: __return_single_value_end__
```

To return multiple values when you know the number of returns before calling the task, use the {ref}`num_returns <ray-task-returns>` option.

```{literalinclude} ../doc_code/anti_pattern_return_ray_put.py
:language: python
:start-after: __return_static_multi_values_start__
:end-before: __return_static_multi_values_end__
```

If you don't know the number of returns before calling the task, use the {ref}`dynamic generator <dynamic-generators>` pattern if possible.

```{literalinclude} ../doc_code/anti_pattern_return_ray_put.py
:language: python
:start-after: __return_dynamic_multi_values_start__
:end-before: __return_dynamic_multi_values_end__
```
