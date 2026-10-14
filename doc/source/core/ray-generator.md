---
myst:
  html_meta:
    description: "Ray generators that yield values incrementally from tasks and actor methods, with error handling, asyncio, cancellation, and fault tolerance."
---

(generators)=

# Ray generators
## Choosing a generator API

Ray's streaming generator API, also called the regular generator API, is the recommended way to consume generator results. It returns an `ObjectRefGenerator`. Use it when a task yields a known or naturally bounded stream of results. You can iterate over it, call `ray.get` on each object reference, or pass it to `ray.wait`.

You need the deprecated dynamic generator API only to support existing code that sets `num_returns="dynamic"`. It returns a `DynamicObjectRefGenerator` through a single `ObjectRef`. In new code, use the streaming generator API instead. Ray plans to remove the dynamic generator API in a future version. For details, see {ref}`Dynamic generators <dynamic_generators>`.

[Python generators](https://docs.python.org/3/howto/functional.html#generators) are functions that behave like iterators, yielding one value per iteration. Ray also supports the Python generator API.

Any generator function decorated with `ray.remote` becomes a Ray generator task. Generator tasks stream outputs back to the caller before the task finishes.

```diff
+import ray
 import time

 # Takes 25 seconds to finish.
+@ray.remote
 def f():
     for i in range(5):
         time.sleep(5)
         yield i

-for obj in f():
+for obj_ref in f.remote():
     # Prints every 5 seconds and stops after 25 seconds.
-    print(obj)
+    print(ray.get(obj_ref))
```

The preceding Ray generator yields an output every 5 seconds, five times. With a normal Ray task, you have to wait 25 seconds to access the output. With a Ray generator, the caller can access the object reference before the task `f` finishes.

Use a Ray generator in the following cases:

- You want to reduce heap memory or object store memory usage by yielding and garbage-collecting the output before the task finishes.
- You're familiar with Python generators and want an equivalent programming model.

Ray libraries use Ray generators to support streaming use cases:

- {ref}`Ray Serve <rayserve>` uses Ray generators to support {ref}`streaming responses <serve-http-streaming-response>`.
- {ref}`Ray Data <data>` is a streaming data processing library that uses Ray generators to control and reduce concurrent memory usage.

Ray generators work with existing Ray APIs:

- You can use Ray generators in both actor and non-actor tasks.
- Ray generators work with all actor execution models, including {ref}`threaded actors <threaded-actors>` and {ref}`async actors <async-actors>`.
- Ray generators work with built-in {ref}`fault tolerance features <fault-tolerance>` such as retry or lineage reconstruction.
- Ray generators work with Ray APIs such as {ref}`ray.wait <generators-wait>` and {ref}`ray.cancel <generators-cancel>`.

## Getting started
Define a Python generator function and decorate it with `ray.remote` to create a Ray generator.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_define_start__
:end-before: __streaming_generator_define_end__
```

A Ray generator task returns an `ObjectRefGenerator` object, which is compatible with the generator and async generator APIs. You can use the `next`, `__iter__`, `__anext__`, and `__aiter__` APIs from the class.

Each time a task invokes `yield`, the corresponding output becomes available from the generator as a Ray object reference. Call `next(gen)` to get an object reference. If `next` has no more items to generate, it raises `StopIteration`. If `__anext__` has no more items to generate, it raises `StopAsyncIteration`.

The `next` API blocks the thread until the task generates the next object reference with `yield`. You can also iterate over object references with a `for` loop.

To avoid blocking a thread, use `asyncio` or the {ref}`ray.wait API <generators-wait>`.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_execute_start__
:end-before: __streaming_generator_execute_end__
```

:::{note}
A normal Python generator function pauses and resumes each time you call `next` on the generator. Ray eagerly executes a generator task to completion, regardless of whether the caller polls the partial results.
:::

## Error handling

If a generator task fails because of an application exception or a system error, such as an unexpected node failure, `next(gen)` returns an object reference that contains an exception. When you call `ray.get` on that object reference, Ray raises the exception.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_exception_start__
:end-before: __streaming_generator_exception_end__
```

In the preceding example, if the application fails the task, Ray returns the object reference with the exception in the correct order. For example, if Ray raises the exception after the second yield, the third `next(gen)` always returns an object reference with an exception. If a system error, such as a node failure or worker process failure, fails the task, `next(gen)` returns the object reference that contains the system-level exception at any time, without an ordering guarantee. As a result, when a generator has N yields and failures occur, it can create from 1 to N + 1 object references, up to N outputs and one object reference that contains the system-level exception.

## Generator from actor tasks
The Ray generator is compatible with all actor execution models. It works with regular actors, {ref}`async actors <async-actors>`, and {ref}`threaded actors <threaded-actors>`.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_actor_model_start__
:end-before: __streaming_generator_actor_model_end__
```

## Using the Ray generator with asyncio
The returned `ObjectRefGenerator` is also compatible with `asyncio`. You can use `__anext__` or `async for` loops.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_asyncio_start__
:end-before: __streaming_generator_asyncio_end__
```

## Garbage collection of object references
The object reference that `next(generator)` returns is a regular Ray object reference, and Ray applies the same distributed reference counting to it. If you don't consume references from a generator with the `next` API, those references are garbage-collected when the generator is garbage-collected.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_gc_start__
:end-before: __streaming_generator_gc_end__
```

In the preceding example, Ray counts `ref1` as a normal Ray object reference after Ray returns it. Other references that you don't consume with `next(gen)` are removed when the generator is garbage-collected. In this example, garbage collection happens when you call `del gen`.

## Fault tolerance
{ref}`Fault tolerance features <fault-tolerance>` work with Ray generator tasks and actor tasks. For example, the following features work with Ray generators:

- {ref}`Task fault tolerance features <task-fault-tolerance>`: `max_retries`, `retry_exceptions`
- {ref}`Actor fault tolerance features <actor-fault-tolerance>`: `max_restarts`, `max_task_retries`
- {ref}`Object fault tolerance features <object-fault-tolerance>`: object reconstruction

(generators-cancel)=

## Cancellation
The {func}`ray.cancel() <ray.cancel>` function works with both Ray generator tasks and actor tasks. Semantically, canceling a generator task is the same as canceling a regular task. When you cancel a task, `next(gen)` can return an object reference that contains {class}`TaskCancelledError <ray.exceptions.TaskCancelledError>` without any special ordering guarantee.

(generators-wait)=

(how-to-wait-for-generator-without-blocking-a-thread-compatibility-to-raywait-and-rayget)=

## Wait for a generator without blocking a thread
The `next` API blocks its thread until the next object reference is available. You can wait for a generator without blocking a thread in three ways.

### Wait until a generator task completes

`ObjectRefGenerator` has a `completed` API, which returns an object reference that becomes available when the generator task finishes or errors. For example, call `ray.get(<generator_instance>.completed())` to wait until the task completes. You can't pass an `ObjectRefGenerator` to `ray.get` directly.

### Use asyncio and await

`ObjectRefGenerator` is compatible with `asyncio`. To avoid blocking a thread, create multiple `asyncio` tasks that create a generator task and wait for it.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_concurrency_asyncio_start__
:end-before: __streaming_generator_concurrency_asyncio_end__
```

### Use ray.wait

You can pass an `ObjectRefGenerator` as an input to `ray.wait`. The generator is "ready" if its next item is available. Once `ray.wait` returns the generator in the ready list, `next(gen)` returns the next object reference immediately without blocking. The following example shows this pattern.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_wait_simple_start__
:end-before: __streaming_generator_wait_simple_end__
```

All of the `ray.wait` input arguments, such as `timeout`, `num_returns`, and `fetch_local`, work with a generator.

You can mix regular Ray object references and generators in the inputs to `ray.wait`. In this case, your application should handle the two kinds of inputs differently. The following example checks whether each ready input is an `ObjectRefGenerator`.

```{literalinclude} doc_code/streaming_generator.py
:language: python
:start-after: __streaming_generator_wait_complex_start__
:end-before: __streaming_generator_wait_complex_end__
```

## Thread safety
`ObjectRefGenerator` objects aren't thread-safe.

## Limitation
Ray generators don't support these features:

- `throw`, `send`, and `close` APIs
- `return` statements from generators
- Passing `ObjectRefGenerator` to another task or actor
- {ref}`Ray Client <ray-client-ref>`

## Deprecated dynamic generator
```{toctree}
:maxdepth: 1

tasks/dynamic-generators
```
