.. _generators:

Ray Generators 
==============

`Python generators <https://docs.python.org/3/howto/functional.html#generators>`_ are functions
that behave like an iterator, yielding one value per iteration. Ray also supports the generators API.

.. code:: python

    import ray
    import time

    # Takes 25 seconds to finish.
    @ray.remote(num_returns="streaming")
    def f():
        for i in range(5):
            time.sleep(5)
            yield i

    for object_ref in f.remote():
        # It is printed every 5 seconds, not after 25 seconds.
        print(ray.get(object_ref))

The above Ray generator yields the output every 5 seconds. The caller can access the object refeference
before the task ``f`` finishes every 5 seconds.

Ray generator is useful when

1. You want to reduce heap memory or object store memory usage by yielding the output before task finishes.
2. You want an easy programming model to access the output before a task finishes. E.g., LLM inference.
3. The number of return values is set dynamically by the remote function
   instead of by the caller.

Here are some real world use cases.

- Ray serve uses Ray generators to support :ref:`streaming responses <serve-http-streaming-response>`.
- Ray data uses Ray generators to 

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
