---
myst:
  html_meta:
    description: "Anti-pattern: processing results in submission order idles on slow tasks; use ray.wait to handle them as they complete."
---

# Anti-pattern: Processing results in submission order using ray.get increases runtime

Avoid processing independent results in submission order using {func}`ray.get() <ray.get>` because results may be ready in a different order than the submission order.

Suppose you submit a batch of tasks and need to process each result once it's done. If each task takes a different amount of time to finish and you process results in submission order, you may waste time waiting for all the slower tasks that you submitted earlier to finish, even though later, faster tasks have already finished. These slower tasks are *stragglers*.

Instead, process the tasks in the order that they finish by using {func}`ray.wait() <ray.wait>` to speed up total time to completion.

```{figure} ../images/ray-get-submission-order.svg
Processing results in submission order versus completion order
```

## Code example

```{literalinclude} ../doc_code/anti_pattern_ray_get_submission_order.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

Other anti-patterns related to `ray.get()` are the following:

- {doc}`unnecessary-ray-get`
- {doc}`ray-get-loop`
