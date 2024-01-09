.. _generators:

Ray Generators 
==============

`Python generators <https://docs.python.org/3/howto/functional.html#generators>`_ are functions
that behave like iterators, yielding one value per iteration. Ray also supports the generators API.

Any generator function decorated with ``ray.remote`` becomes a Ray generator task.
Generator tasks stream outputs back to the caller before the task finishes.

.. code-block:: diff

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


The above Ray generator yields the output every 5 seconds 5 times.
With a normal Ray task, you have to wait 25 seconds to access the output. 
With a Ray generator, the caller can access the object reference
before the task ``f`` finishes.

**The Ray generator is useful when**

- You want to reduce heap memory or object store memory usage by yielding and garbage collecting (GC) the output before the task finishes.
- You are familiar with the Python generator and want the equivalent programming models.

**Ray libraries use the Ray generator to support streaming use cases**

- :ref:`Ray Serve <rayserve>` uses Ray generators to support :ref:`streaming responses <serve-http-streaming-response>`.
- :ref:`Ray Data <data>` is a streaming data processing library, which uses Ray generators to control and reduce concurrent memory usages.

**Ray generator works with existing Ray APIs seamlessly**

- You can use Ray generators in both actor and non-actor tasks.
- Ray generators work with all actor execution models, including :ref:`threaded actors <threaded-actors>` and :ref:`async actors <async-actors>`.
- Ray generators work with built-in :ref:`fault tolerance features <fault-tolerance>` such as retry or lineage reconstruction.
- Ray generators work with Ray APIs such as :ref:`ray.wait <generators-wait>`, :ref:`ray.cancel <generators-cancel>`, etc.

Getting started
---------------
Define a Python generator function and decorate it with ``ray.remote``
to create a Ray generator.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_define_start__
    :end-before: __streaming_generator_define_end__

The Ray generator task returns an ``ObjectRefGenerator`` object, which is
compatible with generator and async generator APIs. You can access the
``next``, ``__iter__``, ``__anext__``, ``__aiter__`` APIs from the class.

Whenever a task invokes ``yield``, a corresponding output is ready and availabale from a generator as a Ray object reference. 
You can call ``next(gen)`` to obtain an object reference.
If ``next`` has no more items to generate, it raises ``StopIteration``. If ``__anext__`` has no more items to generate, it raises
``StopAsyncIteration``

The ``next`` API blocks the thread until the task generates a next object reference with ``yield``.
Since the ``ObjectRefGenerator`` is just a Python generator, you can also use a for loop to
iterate object references. 

If you want to avoid blocking a thread, you can either use asyncio or :ref:`ray.wait API <generators-wait>`.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_execute_start__
    :end-before: __streaming_generator_execute_end__

.. note::

    For a normal Python generator, a generator function is paused and resumed when ``next`` function is
    called on a generator. Ray eagerly executes a generator task to completion regardless of whether the caller is polling the partial results or not.

Error handling
--------------

If a generator task has a failure (by an application exception or system error such as an unexpected node failure),
the ``next(gen)`` returns an object reference that contains an exception. When you call ``ray.get``,
Ray raises the exception.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_exception_start__
    :end-before: __streaming_generator_exception_end__

In the above example, if the an application fails the task, Ray returns the object reference with an exception
in a correct order. For example, if Ray raises the exception after the second yield, the third
``next(gen)`` returns an object reference with an exception all the time. If a system error fails the task,
(e.g., a node failure or worker process failure), ``next(gen)`` returns the object reference that contains the system level exception
at any time without an ordering guarantee.
It means when you have N yields, the generator can create from 1 to N + 1 object references
(N output + ref with a system-level exception) when there failures occur.

Generator from Actor Tasks
--------------------------
The Ray generator is compatible with **all actor execution models**. It seamlessly works with
regular actors, :ref:`async actors <async-actors>`, and :ref:`threaded actors <threaded-actors>`.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_actor_model_start__
    :end-before: __streaming_generator_actor_model_end__

Using the Ray generator with asyncio
------------------------------------
The returned ``ObjectRefGenerator`` is also compatible with asyncio. You can
use ``__anext__`` or ``async for`` loops.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_asyncio_start__
    :end-before: __streaming_generator_asyncio_end__

Garbage collection of object referneces
---------------------------------------
The returned ref from ``next(generator)`` is just a regular Ray object reference and is distribute ref counted in the same way.
If references are not consumed from a generator by the ``next`` API, referencesare garbage collected (GC’ed) when the generator is GC’ed

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_gc_start__
    :end-before: __streaming_generator_gc_end__

In the following example, Ray counts ``ref1`` a normal Ray object reference after Ray returns it. Other references
that aren't consumed with ``next(gen)`` are removed when the generator is GC'ed. In this example, garbage collection happens when you call ``del gen``.

Fault tolerance
---------------
:ref:`Fault tolerance features <fault-tolerance>` work with
Ray generator tasks and actor tasks. For example;

- :ref:`Task fault tolerance features <task-fault-tolerance>`: ``max_retries``, ``retry_exceptions``
- :ref:`Actor fault tolerance features <actor-fault-tolerance>`: ``max_restarts``, ``max_task_retries``
- :ref:`Object fault tolerance features <object-fault-tolerance>`: object reconstruction

.. _generators-cancel:

Cancellation
------------
The :func:`ray.cancel() <ray.cancel>` function works with both Ray generator tasks and actor tasks.
Semantic-wise, cancelling a generator task isn't different from cancelling a regular task.
When you cancel a task, ``next(gen)`` can return the reference that contains :class:`TaskCancelledError <ray.exceptions.TaskCancelledError>` without any special ordering guarantee.

.. _generators-wait:

How to wait for generator without blocking a thread (compatibility to ray.wait and ray.get)
-------------------------------------------------------------------------------------------
When using a generator, ``next`` API blocks its thread until a next object reference is available.
However, you may not want this behavior all the time. You may want to wait for a generator without blocking a thread.
Unblocking wait is possible with the Ray generator in the following ways:

**Wait until a generator task completes**

``ObjectRefGenerator`` has an API ``completed``. It returns an object reference that is available when a generator task finishes or errors.
For example, you can do ``ray.get(<generator_instance>.compelted())`` to wait until a task completes. Note that using ``ray.get`` to ``ObjectRefGenerator`` isn't allowed.

**Use asyncio and await**

``ObjectRefGenerator`` is compatible with asyncio. You can create multiple asyncio tasks that create a generator task
and wait for it to avoid blocking a thread.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_concurrency_asyncio_start__
    :end-before: __streaming_generator_concurrency_asyncio_end__

**Use ray.wait**

You can pass ``ObjectRefGenerator`` as an input to ``ray.wait``. The generator is "ready" if a `next item`
is available. Once Ray finds from a ready list, ``next(gen)`` returns the next object reference immediately without blocking. See the example below for more details.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_wait_simple_start__
    :end-before: __streaming_generator_wait_simple_end__

All the input arguments (such as ``timeout``, ``num_returns``, and ``fetch_local``) from ``ray.wait`` works with a generator.

``ray.wait`` can mix regular Ray object references with generators for inputs. In this case, the application should handle
all input arguments (such as ``timeout``, ``num_returns``, and ``fetch_local``) from ``ray.wait`` work with generators.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_wait_complex_start__
    :end-before: __streaming_generator_wait_complex_end__

Thread safety
-------------
``ObjectRefGenerator`` object is not thread-safe.

Limitation
----------
Ray generators don't support these features:

- ``throw``, ``send``, and ``close`` APIs.
- ``return`` statements from generators.
- Passing ``ObjectRefGenerator`` to another task or actor.
