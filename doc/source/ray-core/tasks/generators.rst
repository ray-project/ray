.. _generators:

Generators
==========

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

.. note:: ``num_returns="dynamic"`` is currently an experimental API in v2.1+.

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

``num_returns="dynamic"`` is not yet supported for :ref:`Ray Client <ray-client-ref>`.
