---
myst:
  html_meta:
    description: "Anti-pattern: calling ray.get inside a loop blocks on each task in turn and destroys parallelism; collect the refs first."
---

(ray-get-loop)=

# Anti-pattern: Calling ray.get in a loop harms parallelism

Avoid calling {func}`ray.get() <ray.get>` in a loop, because it's a blocking call. Use `ray.get()` only for the final result.

A call to `ray.get()` fetches the results of remotely executed functions. However, it's a blocking call, so it always waits until the requested result is available. If you call `ray.get()` in a loop, the loop doesn't continue until the call to `ray.get()` resolves.

If you also spawn the remote function calls in the same loop, you end up with no parallelism at all. `ray.get()` makes you wait for the previous function call to finish, and you spawn the next call only in the next iteration of the loop. Instead, separate the call to `ray.get()` from the calls to the remote functions. That way, you spawn all remote functions before you wait for the results, and they can run in parallel in the background. You can also pass a list of object refs to `ray.get()` to wait for all of the tasks to finish, instead of calling it on each ref one by one.

## Code example

```{literalinclude} ../doc_code/anti_pattern_ray_get_loop.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

```{figure} ../images/ray-get-loop.svg
Calling `ray.get()` in a loop
```

When you call `ray.get()` right after scheduling the remote work, the loop blocks until the result arrives, so processing is sequential. Instead, schedule all remote calls first so that they run in parallel. After scheduling the work, request all the results at once.

Other anti-patterns related to `ray.get()` are the following:

- {doc}`nested-ray-get`
- {doc}`unnecessary-ray-get`
- {doc}`ray-get-submission-order`
- {doc}`ray-get-too-many-objects`
