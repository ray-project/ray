---
myst:
  html_meta:
    description: "Anti-pattern: calling ray.get before the value is needed blocks the driver and forfeits overlap between tasks."
---

(unnecessary-ray-get)=

# Anti-pattern: Calling ray.get unnecessarily harms performance

Avoid calling {func}`ray.get() <ray.get>` unnecessarily for intermediate steps. Work with object refs directly, and call `ray.get()` only at the end to get the final result.

When you call `ray.get()`, the objects must be transferred to the worker or node that makes the call. If you don't need to manipulate an object, you probably don't need to call `ray.get()` on it.

Typically, wait as long as possible before you call `ray.get()`, or design your program so that it doesn't need to call `ray.get()` at all.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_unnecessary_ray_get.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

```{figure} ../images/unnecessary-ray-get-anti.svg
```

**Better approach:**

```{literalinclude} ../doc_code/anti_pattern_unnecessary_ray_get.py
:language: python
:start-after: __better_approach_start__
:end-before: __better_approach_end__
```

```{figure} ../images/unnecessary-ray-get-better.svg
```

In the anti-pattern example, the call to `ray.get()` forces the large rollout to be transferred to the driver, and then again to the `reduce` worker.

In the fixed version, you pass only the object ref to the `reduce` task. The `reduce` worker implicitly calls `ray.get()` to fetch the rollout data directly from the `generate_rollout` worker, which avoids the extra copy to the driver.

Other anti-patterns related to `ray.get()` are the following:

- {doc}`ray-get-loop`
- {doc}`ray-get-submission-order`
