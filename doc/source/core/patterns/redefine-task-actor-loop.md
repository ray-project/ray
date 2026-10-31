---
myst:
  html_meta:
    description: "Anti-pattern: redefining the same remote function or actor class in a loop re-exports it each time, adding overhead."
---

# Anti-pattern: Redefining the same remote function or class harms performance

Avoid redefining the same remote function or class.

Decorating the same function or class multiple times with the {func}`ray.remote <ray.remote>` decorator leads to slow performance in Ray. For each remote function or class, Ray pickles it and uploads it to GCS. Later, the worker that runs the task or actor downloads and unpickles it. From Ray's perspective, each decoration of the same function or class generates a new remote function or class. As a result, the pickle, upload, download, and unpickle work happens every time you redefine and run the remote function or class.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_redefine_task_actor_loop.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach:**

```{literalinclude} ../doc_code/anti_pattern_redefine_task_actor_loop.py
:language: python
:start-after: __better_approach_start__
:end-before: __better_approach_end__
```

Define the remote function or class once outside the loop, instead of multiple times inside it, so that Ray pickles and uploads it only once.
