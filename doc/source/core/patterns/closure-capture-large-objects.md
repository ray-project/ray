---
myst:
  html_meta:
    description: "Anti-pattern: capturing large objects in a remote function's closure copies them to every worker; put them in the object store instead."
---

# Anti-pattern: Closure capturing large objects harms performance

Don't capture large objects in the closure of a remote function or class. Use the object store instead.

When you define a {func}`ray.remote <ray.remote>` function or class, it's easy to accidentally capture large objects of more than a few MB implicitly in the definition. This can lead to slow performance or even out-of-memory (OOM) errors, because Ray isn't designed to handle large serialized functions or classes.

To resolve this problem for large objects, use one of the following two options:

- Use {func}`ray.put() <ray.put>` to put the large objects in the Ray object store, and then pass object refs as arguments to the remote functions or classes. *Better approach #1* in the code example shows this option.
- Create the large objects inside the remote functions or classes by passing a lambda method. *Better approach #2* shows this option. It's also the only option for using unserializable objects.

## Code example

**Anti-pattern:**

```{literalinclude} ../doc_code/anti_pattern_closure_capture_large_objects.py
:language: python
:start-after: __anti_pattern_start__
:end-before: __anti_pattern_end__
```

**Better approach #1:**

```{literalinclude} ../doc_code/anti_pattern_closure_capture_large_objects.py
:language: python
:start-after: __better_approach_1_start__
:end-before: __better_approach_1_end__
```

**Better approach #2:**

```{literalinclude} ../doc_code/anti_pattern_closure_capture_large_objects.py
:language: python
:start-after: __better_approach_2_start__
:end-before: __better_approach_2_end__
```
