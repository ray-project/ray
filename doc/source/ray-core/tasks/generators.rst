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


`num_returns` set by the task caller
------------------------------------

Where possible, the caller should set the remote function's number of return values using ``@ray.remote(num_returns=x)`` or ``foo.options(num_returns=x).remote()``.
Ray will return this many ``ObjectRefs`` to the caller.
The remote task should then return the same number of values, usually as a tuple or list.

Without changing the caller's syntax, we can also use a remote generator function to return the values iteratively.
The generator should return the same number of return values specified by the caller, and these will be stored one at a time in Ray's object store.
An error will be returned for generators that return a different number of values from the one specified by the caller.

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
It can return the arrays one at a time to reduce memory pressure.

`num_returns` set by the task executor
--------------------------------------

In some cases, the caller may not know the number of return values to expect from a remote function.
For example, suppose we want to write a task that breaks up its argument into equal-size chunks and returns these.
We may not know the size of the argument until we execute the task, so we don't know the number of return values to expect.

In these cases, we can use a remote generator function that returns a *dynamic* number of values.
To use this feature, set ``num_returns="dynamic"`` in the ``@ray.remote`` decorator or the remote function's ``.options()``.
Then, when invoking the remote function, Ray will return an ``ObjectRefGenerator`` instead of one or multiple ``ObjectRefs``.

Note that when an ``ObjectRefGenerator`` is first iterated upon, *the caller will block until the corresponding task is complete* and the number of return values is known.
Ray will then populate the generator with a list of ``ObjectRefs`` pointing to the values returned by the remote function.
Then, the caller can iterate through the generator like any other list of ``ObjectRefs``.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __dynamic_generator_start__
    :end-before: __dynamic_generator_end__

We can also get the return values as a generator by passing the ``ObjectRefGenerator`` to ``ray.get``.
 This will return another generator, this time of the **values** of the contained ``ObjectRefs``.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __dynamic_generator_ray_get_start__
    :end-before: __dynamic_generator_ray_get_end__

Finally, the semantics for passing an ``ObjectRefGenerator`` to another remote function are similar to that of passing a list of ``ObjectRefs``.
 The remote task worker will receive the same ``ObjectRefGenerator``, which it can iterate over directly or pass to ``ray.get`` or another task.

.. literalinclude:: ../doc_code/generator.py
    :language: python
    :start-after: __dynamic_generator_pass_start__
    :end-before: __dynamic_generator_pass_end__
