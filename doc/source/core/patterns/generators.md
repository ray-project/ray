---
myst:
  html_meta:
    description: "Pattern: yield results from a generator task instead of returning them all at once, to cap heap memory usage."
---

(generator-pattern)=

# Pattern: Using generators to reduce heap memory usage

In this pattern, you use Python *generators* to reduce the total heap memory usage during a task. The key idea is that a task that returns multiple objects can return them one at a time instead of all at once. A worker can then free the heap memory that a previous return value used before it returns the next one.

## Example use case

You have a task that returns multiple large values. Alternatively, you have a task that returns a single large value, and you want to stream that value through Ray's object store by breaking it into smaller chunks.

The following example writes such a task as a normal Python function that returns NumPy arrays of 100 MB each:

```{literalinclude} ../doc_code/pattern_generators.py
:language: python
:start-after: __large_values_start__
:end-before: __large_values_end__
```

However, this approach requires the task to hold all `num_returns` arrays in heap memory at the same time at the end of the task. With many return values, this can lead to high heap memory usage and potentially an out-of-memory error.

To fix the preceding example, rewrite `large_values` as a generator. Instead of returning all values at once as a tuple or list, a generator can `yield` one value at a time.

```{literalinclude} ../doc_code/pattern_generators.py
:language: python
:start-after: __large_values_generator_start__
:end-before: __large_values_generator_end__
```

## Code example

```{literalinclude} ../doc_code/pattern_generators.py
:language: python
:start-after: __program_start__
```

```text
$ RAY_IGNORE_UNHANDLED_ERRORS=1 python test.py 100

Using normal functions...
... -- A worker died or was killed while executing a task by an unexpected system error. To troubleshoot the problem, check the logs for the dead worker...
Worker failed
Using generators...
(large_values_generator pid=373609) yielded return value 0
(large_values_generator pid=373609) yielded return value 1
(large_values_generator pid=373609) yielded return value 2
...
Success!
```
