---
myst:
  html_meta:
    description: "Anti-pattern: passing the same large argument by value to many tasks re-serializes it each time; ray.put it once instead."
---

(ray-pass-large-arg-by-value)=

# Anti-pattern: Passing the same large argument by value repeatedly harms performance

Avoid passing the same large argument by value to multiple tasks. Use {func}`ray.put() <ray.put>` and pass by reference instead.

When you pass an argument larger than 100 KB by value to a task, Ray implicitly stores the argument in the object store. Before the task runs, the worker process fetches the argument from the caller's object store to the local object store. If you pass the same large argument to multiple tasks, Ray stores multiple copies of the argument in the object store, because Ray doesn't deduplicate them.

Instead of passing the large argument by value to multiple tasks, call `ray.put()` once to store the argument in the object store and get an object ref. Then pass that object ref to the tasks. All tasks use the same copy of the argument, which is faster and uses less object store memory.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_pass_large_arg_by_value.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach:**

```{literalinclude} ../doc_code/anti_pattern_pass_large_arg_by_value.py
:language: python
:start-after: __better_approach_start__
:end-before: __better_approach_end__
```
