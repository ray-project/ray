.. _generators:

Generators
==========

.. note::

  ``num_returns="streaming"`` API will replace ``num_returns="dynamic"`` :ref:`generator API <dynamic_generators>`, which will be deprecated from Ray 2.8.
  The API is experimental, and APIs subject to change. However, the feature is already used to support ray data and ray serve streaming use cases.

`Python generators <https://docs.python.org/3/howto/functional.html#generators>`_ are functions
that behave like an iterator, yielding one value per iteration. Ray supports the generators API.

.. code:: python

    import ray
    import time

    @ray.remote(num_returns="streaming")
    def f():
        for i in range(5):
            time.sleep(5)
            yield i

    for object_ref in f.remote():
        print(ray.get(object_ref))

The above Ray generator yields the output every 5 seconds, and the caller can access the object refeference
before the task ``f`` finishes. Ray generator is useful when

1. You want to reduce heap memory or object store memory usage by yielding the output before task finishes.
2. The number of return values is set dynamically by the remote function
   instead of by the caller.

Ray generators can be used in both actor and non-actor tasks.

Getting Started
---------------
You have to define a Python generator function and specify ``num_returns="streaming"`` 
to create a Ray generators.

.. literalinclude:: ../doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_define_start__
    :end-before: __streaming_generator_define_end__

The ray generator task returns ``StreamingObjectRefGenerator`` (API is subject to change), which is
compatible to generator and async generator APIs. It means 
``next``, ``__iter__``, ``__anext__``, ``__aiter__`` APIs are accessible from the class. You
can call ``next(gen)`` to obtain a object reference yielded from a generator task. 
``next`` API will block until the next item is generated from the task. You can also use a for loop
to go through all yielded object references until the task is finished.

.. literalinclude:: ../doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_execute_start__
    :end-before: __streaming_generator_execute_end__

.. note::

    For a normal Python generator, a generator function is paused and resumed when ``next`` function is
    called on a generator. In Ray, a generator task is eagerly executed and sends the result back to the caller,
    and it is never paused.

Error Handling
--------------

If there's any failure from a generator task (by an application exception or system error such as unexpected node failure),
the ``next(gen)`` will return an object reference that contains an exception. When you call ``ray.get``,
the exception is raised. 

.. literalinclude:: ../doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_exception_start__
    :end-before: __streaming_generator_exception_end__

As you see from the example, If the task is failed by an application, the object reference with an exception
is returned with the correct order (for example, if the exception is raised after the second yield, the third 
``next(gen)`` will return an object reference with an exception). If the task is failed
by a system error (e.g., node failure or worker process failure), the object reference that contains the system level exception
can be raised any time.

Generator from Actor Task
-------------------------
Ray generator is compatible with **all execution model of actors**. I.e., it works with
actor, async actor, and threaded actors.

# SANG-TODO links

Using Ray Generator with Asyncio
--------------------------------
The returned ``StreamingObjectRefGenerator`` is also compatible with asyncio, which means you can
use ``__anext__`` or ``async for`` loop.

.. literalinclude:: ../doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_asyncio_start__
    :end-before: __streaming_generator_asyncio_end__

Garbage Collection of Object Referneces
---------------------------------------
The returned ref from next(generator) will be distributed ref counted like a normal object references. 
If references are not consumed from a generator via ``next``, they are GC’ed when the generator is GC’ed

# SANG-TODO example
generator = split.remote()
ref1 = next(generator)
del generator
# ref2 is GC’ed when generator is GC’ed. ref1 survives
# the second and third refs will be GC'ed.

Lineage Reconstruction and Retry
--------------------------------
Every fault tolerance feature from #SANG-TODO<link to doc> is also working with
Ray generator tasks and actor tasks.

# SANG-TODO List all APIs

Cancelation
-----------
:func:`ray.cancel() <ray.cancel>` works with both Ray generator tasks and actor tasks.

Compatibility to ray.wait and ray.get
-------------------------------------
# SANG-TODO

Limitation
----------
There are some features that are not supported from Ray generators.

- ``throw``, ``send``, and ``close`` APIs are not supported.
- ``return`` statement from a generator is not supported.
- Passing an ``StreamingObjectRefGenerator`` to another task or actor is not allowed.


.. _dynamic_generators:

Dynamic Generators
==================

Python generators are functions that behave like an iterator, yielding one
value per iteration. Ray supports remote generators for two use cases:

1. To reduce max heap memory usage when returning multiple values from a remote
   function. See the :ref:`design pattern guide <generator-pattern>` for an
   example.
2. When the number of return values is set dynamically by the remote function
   instead of by the caller.

Remote generators can be used in both actor and non-actor tasks.

.. _static-generators:

`num_returns` set by the task caller
------------------------------------

Where possible, the caller should set the remote function's number of return values using ``@ray.remote(num_returns=x)`` or ``foo.options(num_returns=x).remote()``.
Ray will return this many ``ObjectRefs`` to the caller.
The remote task should then return the same number of values, usually as a tuple or list.
Compared to setting the number of return values dynamically, this adds less complexity to user code and less performance overhead, as Ray will know exactly how many ``ObjectRefs`` to return to the caller ahead of time.

Without changing the caller's syntax, we can also use a remote generator function to yield the values iteratively.
The generator should yield the same number of return values specified by the caller, and these will be stored one at a time in Ray's object store.
An error will be raised for generators that yield a different number of values from the one specified by the caller.

For example, we can swap the following code that returns a list of return values:

.. literalinclude:: ../doc_code/pattern_generators.py
    :language: python
    :start-after: __large_values_start__
    :end-before: __large_values_end__

for this code, which uses a generator function:

.. literalinclude:: ../doc_code/pattern_generators.py
    :language: python
    :start-after: __large_values_generator_start__
    :end-before: __large_values_generator_end__

The advantage of doing so is that the generator function does not need to hold all of its return values in memory at once.
It can yield the arrays one at a time to reduce memory pressure.

.. _dynamic-generators:

`num_returns` set by the task executor
--------------------------------------

In some cases, the caller may not know the number of return values to expect from a remote function.
For example, suppose we want to write a task that breaks up its argument into equal-size chunks and returns these.
We may not know the size of the argument until we execute the task, so we don't know the number of return values to expect.

In these cases, we can use a remote generator function that returns a *dynamic* number of values.
To use this feature, set ``num_returns="dynamic"`` in the ``@ray.remote`` decorator or the remote function's ``.options()``.
Then, when invoking the remote function, Ray will return a *single* ``ObjectRef`` that will get populated with an ``ObjectRefGenerator`` when the task completes.
The ``ObjectRefGenerator`` can be used to iterate over a list of ``ObjectRefs`` containing the actual values returned by the task.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __dynamic_generator_start__
    :end-before: __dynamic_generator_end__

We can also pass the ``ObjectRef`` returned by a task with ``num_returns="dynamic"`` to another task. The task will receive the ``ObjectRefGenerator``, which it can use to iterate over the task's return values. Similarly, you can also pass an ``ObjectRefGenerator`` as a task argument.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __dynamic_generator_pass_start__
    :end-before: __dynamic_generator_pass_end__

Exception handling
------------------

If a generator function raises an exception before yielding all its values, the values that it already stored will still be accessible through their ``ObjectRefs``.
The remaining ``ObjectRefs`` will contain the raised exception.
This is true for both static and dynamic ``num_returns``.
If the task was called with ``num_returns="dynamic"``, the exception will be stored as an additional final ``ObjectRef`` in the ``ObjectRefGenerator``.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __generator_errors_start__
    :end-before: __generator_errors_end__

Note that there is currently a known bug where exceptions will not be propagated for generators that yield more values than expected. This can occur in two cases:

1. When ``num_returns`` is set by the caller, but the generator task returns more than this value.
2. When a generator task with ``num_returns="dynamic"`` is :ref:`re-executed <task-retries>`, and the re-executed task yields more values than the original execution. Note that in general, Ray does not guarantee correctness for task re-execution if the task is nondeterministic, and it is recommended to set ``@ray.remote(num_retries=0)`` for such tasks.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __generator_errors_unsupported_start__
    :end-before: __generator_errors_unsupported_end__

Limitations
-----------

Although a generator function creates ``ObjectRefs`` one at a time, currently Ray will not schedule dependent tasks until the entire task is complete and all values have been created. This is similar to the semantics used by tasks that return multiple values as a list.
