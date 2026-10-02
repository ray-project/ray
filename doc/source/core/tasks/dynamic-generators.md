---
myst:
  html_meta:
    description: "Ray remote generators with a dynamic number of returns, set by either the caller or the executor, plus exception handling and limits."
---

(dynamic_generators)=

# Dynamic generators


:::{warning}
`num_returns="dynamic"` {ref}`generator API <dynamic_generators>` is deprecated and scheduled for removal in an upcoming version. Use the {ref}`streaming generator API <generators>` instead. Ray emits a runtime `RayDeprecationWarning` when you use `num_returns="dynamic"`.
:::

Python generators are functions that behave like iterators, yielding one value per iteration. Ray supports remote generators for two use cases:

1. Reducing maximum heap memory usage when a remote function returns multiple values. See the {ref}`design pattern guide <generator-pattern>` for an example.
1. Returning a number of values that the remote function sets dynamically, instead of a number that the caller sets.

You can use remote generators in both actor and non-actor tasks.

(static-generators)=

## `num_returns` set by the task caller

Where possible, set the remote function's number of return values from the caller with `@ray.remote(num_returns=x)` or `foo.options(num_returns=x).remote()`. Ray returns this many `ObjectRefs` to the caller, and the remote task should return the same number of values, usually as a tuple or list. Compared to setting the number of return values dynamically, this approach adds less complexity to your code and less performance overhead, because Ray knows ahead of time exactly how many `ObjectRefs` to return to the caller.

Without changing the caller's syntax, you can also use a remote generator function to yield the values iteratively. The generator should yield the same number of return values that the caller specifies, and Ray stores them one at a time in its object store. If a generator yields a different number of values than the caller specifies, Ray raises an error.

For example, you can replace the following code, which returns a list of return values:

```{literalinclude} ../doc_code/pattern_generators.py
:language: python
:start-after: __large_values_start__
:end-before: __large_values_end__
```

with this code, which uses a generator function:

```{literalinclude} ../doc_code/pattern_generators.py
:language: python
:start-after: __large_values_generator_start__
:end-before: __large_values_generator_end__
```

With this change, the generator function doesn't need to hold all of its return values in memory at once. It can yield the arrays one at a time to reduce memory pressure.

(dynamic-generators)=

## `num_returns` set by the task executor

In some cases, the caller doesn't know how many return values to expect from a remote function. For example, suppose you want to write a task that breaks up its argument into equal-size chunks and returns them. You might not know the size of the argument until the task executes, so you don't know how many return values to expect.

In these cases, you can use a remote generator function that returns a *dynamic* number of values. To use this feature, set `num_returns="dynamic"` in the `@ray.remote` decorator or the remote function's `.options()`. When you invoke the remote function, Ray returns a *single* `ObjectRef`, which it populates with a `DynamicObjectRefGenerator` when the task completes. Use the `DynamicObjectRefGenerator` to iterate over a list of `ObjectRefs` that contain the actual values the task returned.

```{literalinclude} ../doc_code/generator.py
:language: python
:start-after: __dynamic_generator_start__
:end-before: __dynamic_generator_end__
```

You can also pass the `ObjectRef` that a task with `num_returns="dynamic"` returns to another task. The receiving task gets the `DynamicObjectRefGenerator`, which it can use to iterate over the original task's return values. Similarly, you can pass the `DynamicObjectRefGenerator` itself as a task argument.

```{literalinclude} ../doc_code/generator.py
:language: python
:start-after: __dynamic_generator_pass_start__
:end-before: __dynamic_generator_pass_end__
```

## Exception handling

If a generator function raises an exception before yielding all its values, the values that it already stored remain accessible through their `ObjectRefs`. The remaining `ObjectRefs` contain the raised exception. This behavior applies to both static and dynamic `num_returns`. If you call the task with `num_returns="dynamic"`, Ray stores the exception as an additional final `ObjectRef` in the `DynamicObjectRefGenerator`.

```{literalinclude} ../doc_code/generator.py
:language: python
:start-after: __generator_errors_start__
:end-before: __generator_errors_end__
```

A known bug currently prevents Ray from propagating exceptions for generators that yield more values than expected. This bug can occur in two cases:

1. When the caller sets `num_returns`, but the generator task returns more values than that number.
1. When Ray {ref}`re-executes <task-retries>` a generator task with `num_returns="dynamic"`, and the re-executed task yields more values than the original execution.

In general, Ray doesn't guarantee correctness for re-executing a nondeterministic task, so set `@ray.remote(max_retries=0)` for such tasks.

```{literalinclude} ../doc_code/generator.py
:language: python
:start-after: __generator_errors_unsupported_start__
:end-before: __generator_errors_unsupported_end__
```

(dynamic-generators-limitation)=

## Limitations

Although a generator function creates `ObjectRefs` one at a time, Ray currently doesn't schedule dependent tasks until the entire task completes and has created all its values. This behavior is similar to the semantics of tasks that return multiple values as a list.
