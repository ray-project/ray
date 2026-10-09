---
myst:
  html_meta:
    description: "Anti-pattern: fetching too many objects at once with ray.get can exhaust the object store or heap and fail the job."
---

(ray-get-too-many-objects)=

# Anti-pattern: Fetching too many objects at once with ray.get causes failure

Avoid calling {func}`ray.get() <ray.get>` on too many objects, because this can lead to a heap out-of-memory or object store out-of-space failure. Instead, fetch and process one batch at a time.

If you have many tasks that you want to run in parallel, calling `ray.get()` on all of them at once could fail with a heap out-of-memory or object store out-of-space error, because Ray needs to fetch all the objects to the caller at the same time. Instead, get and process the results one batch at a time. After you process a batch, Ray evicts the objects in that batch to make space for later batches.

```{figure} ../images/ray-get-too-many-objects.svg
Fetching too many objects at once with `ray.get()`
```

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_ray_get_too_many_objects.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach:**

```{literalinclude} ../doc_code/anti_pattern_ray_get_too_many_objects.py
:language: python
:start-after: __better_approach_start__
:end-before: __better_approach_end__
```

Besides getting one batch at a time to avoid failure, this example also uses `ray.wait()` to reduce the runtime by processing results in the order they finish instead of the order you submitted them. For more details, see {doc}`ray-get-submission-order`.
