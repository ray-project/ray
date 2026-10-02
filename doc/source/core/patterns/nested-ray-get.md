---
myst:
  html_meta:
    description: "Anti-pattern: calling ray.get on task arguments serializes execution; pass object refs and let Ray resolve them."
---

(nested-ray-get)=

# Anti-pattern: Calling ray.get on task arguments harms performance

If possible, pass object refs directly as task arguments instead of passing a list as the task argument and then calling {func}`ray.get() <ray.get>` inside the task.

When a task calls `ray.get()`, it blocks until the value of the object ref is ready. If all cores are already occupied, this blocking can lead to a deadlock, because the task that produces the object ref's value may need the caller task's resources to run. To handle this case, Ray temporarily releases the caller's CPU resources when the caller would block in `ray.get()`, so the pending task can run. This behavior can harm performance and stability, because the caller still occupies a process and uses memory to hold its stack while other tasks run.

When possible, pass object refs directly as task arguments and avoid calling `ray.get` inside the task.

For example, the following code shows two ways to invoke the dependent task. Use the second one.

```{literalinclude} ../doc_code/anti_pattern_nested_ray_get.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

Avoiding `ray.get` in nested tasks isn't always possible. Valid reasons to call `ray.get` inside a task include the following:

- The task uses {doc}`nested tasks <nested-tasks>` to achieve nested parallelism.
- The nested task has multiple object refs to pass to `ray.get`, and it wants to choose the order and number of objects to get.
