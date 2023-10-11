.. _generators:

Ray Generators 
==============

`Python generators <https://docs.python.org/3/howto/functional.html#generators>`_ are functions
that behave like an iterator, yielding one value per iteration. Ray also supports the generators API.

.. code-block:: diff

    +import ray
     import time

     # Takes 25 seconds to finish.
    +@ray.remote(num_returns="streaming")
     def f():
         for i in range(5):
             time.sleep(5)
             yield i

    -for obj in f():
    +for obj_ref in f.remote():
         # It is printed every 5 seconds, not after 25 seconds.
    -    print(obj)
    +    print(ray.get(obj_ref))


The above Ray generator yields the output every 5 seconds 5 times.
When you use a normal Ray task, you have to wait 25 seconds to access the output. 
With a Ray generator, the caller can access the object refeference
before the task ``f`` finishes.

**Ray generator is useful when**

- You want to reduce heap memory or object store memory usage by yielding and GC the output before task finishes.
- You are familiar with Python generator and want the equivalent programming models.

**Ray generator is used by Ray libraries to support stremaing use cases**

- :ref:`Ray serve <rayserve>` uses Ray generators to support :ref:`streaming responses <serve-http-streaming-response>`.
- :ref:`Ray data <data>` is a streaming data processing library, and it uses Ray generators to control and reduce concurrent memory usages.

**Ray generator works with existing Ray APIs seamlessly**

- Ray generators can be used in both actor and non-actor tasks.
- Ray generators work with all actor execution model, including :ref:`threaded actors <threaded-actors>` and :ref:`async actors <async-actors>`.
- Ray generators work with built-in :ref:`fault tolerance features <fault-tolerance>` such as retry or lineage reconstruction.
- Ray generators work with Ray APIs such as :ref:`ray.wait <generators-wait>`, :ref:`ray.cancel <generators-cancel>`, etc.

Getting Started
---------------
You have to define a Python generator function and specify ``num_returns="streaming"`` 
to create a Ray generators.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_define_start__
    :end-before: __streaming_generator_define_end__

The ray generator task returns ``StreamingObjectRefGenerator`` (API is subject to change), which is
compatible to generator and async generator APIs. It means 
``next``, ``__iter__``, ``__anext__``, ``__aiter__`` APIs are accessible from the class. 

Whenever a task invokes ``yield``, a corresponding output is ready and availabale from a generator as a Ray object reference. 
You can call ``next(gen)`` to obtain a object reference.
If ``next`` has no more item to generate, it raises ``StopIteration``. If ``__anext__`` has no more item to generate, it raises
``StopAsyncIteration``

``next`` API will block its thread until a next object reference is generated from the task via ``yield``.
Since the ``StreamingObjectRefGenerator`` is just a Python generator, you can also simply use a for loop to
iterate object references. 

If you want to avoid blocking a thread, you can either use asyncio or :ref:`ray.wait API <generators-wait>`.

.. literalinclude:: doc_code/streaming_generator.py
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

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_exception_start__
    :end-before: __streaming_generator_exception_end__

As you see from the example, If the task is failed by an application, the object reference with an exception
is returned with at a correct order (for example, if the exception is raised after the second yield, the third 
``next(gen)`` will return an object reference with an exception all the time). If the task is failed
by a system error (e.g., node failure or worker process failure), the object reference that contains the system level exception
can be returned from ``next(gen)`` any time without an ordering guarantee. 
It means when you have N yields, the generator can create from 1 to N + 1 object references
(N output + ref with a system-level exception) when there are failures.

Generator from Actor Task
-------------------------
Ray generator is compatible with **all execution model of actors**. I.e., it seamlessly works with
regular actor, :ref:`async actors <async-actors>`, and :ref:`threaded actors <threaded-actors>`.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_actor_model_start__
    :end-before: __streaming_generator_actor_model_end__

Using Ray Generator with Asyncio
--------------------------------
The returned ``StreamingObjectRefGenerator`` is also compatible with asyncio, which means you can
use ``__anext__`` or ``async for`` loop.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_asyncio_start__
    :end-before: __streaming_generator_asyncio_end__

Garbage Collection of Object Referneces
---------------------------------------
The returned ref from ``next(generator)`` is just a regular Ray object reference, and it is distribute ref counted in the same way.
If references are not consumed from a generator via ``next``, they are GC’ed when the generator is GC’ed

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_gc_start__
    :end-before: __streaming_generator_gc_end__

In the following example, ``ref1`` is ref counted like a normal Ray object reference after it is returned. Other references
that are not consumed via ``next(gen)`` are GC'ed when a generator is GC'ed (in this example, it happens when ``del gen`` is called).

Fault Tolerance
---------------
Every fault tolerance feature from :ref:`fault tolerance features <fault-tolerance>` is also working with
Ray generator tasks and actor tasks. For example,

- :ref:`Task fault tolerance features <task-fault-tolerance>`: ``max_retries``, ``retry_exceptions``.
- :ref:`Actor fault tolerance features <actor-fault-tolerance>`: ``max_restarts``, ``max_task_retries``.
- :ref:`Object fault tolerance features <object-fault-tolerance>`: object reconstruction.

.. _generators-cancel:

Cancellation
------------
:func:`ray.cancel() <ray.cancel>` works with both Ray generator tasks and actor tasks. 
Semantic-wise, cancelling a generator task has no difference from cancelling a regular task.
When a task is cancelled, the reference that contains :class:`TaskCancelledError <ray.exceptions.TaskCancelledError>`
can be returned from ``next(gen)`` without any special ordering guarantee.

.. _generators-wait:

How to wait for generator without blocking a thread (compatibility to ray.wait and ray.get)
-------------------------------------------------------------------------------------------
When using a generator, ``next`` API will block its thread until a next object reference is available.
However, this is not ideal all the time. Sometimes you'd like to wait for a generator without blocking a thread.
Unblocking wait is possible with ray generator with the following ways;

**Wait until a generator task completes**

``StreamingObjectRefGenerator.completed()`` returns an object reference that is available when a generator task finishes or errors.
For example, you can do ``ray.get(gen.compelted())`` to wait until a task completes. Note that using ``ray.get`` to ``StreamingObjectRefGenerator`` is not allowed.

**Use asyncio and await**

``StreamingObjectRefGenerator`` is compatible to asyncio. You can create multiple asyncio tasks that creates a generator task
and wait for it to avoid blocking a thread.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_concurrency_asyncio_start__
    :end-before: __streaming_generator_concurrency_asyncio_end__

**Use ray.wait**

``StreamingObjectRefGenerator`` could be passed as an input of ``ray.wait``. The generator is considered as "ready" if a next item
is available. Once a generator is found from a ready list, ``next(gen)`` returns the next object reference immediately without blocking. See the below example for more details.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_wait_simple_start__
    :end-before: __streaming_generator_wait_simple_end__

All the input arguments (such as ``timeout``, ``num_returns``, and ``fetch_local``) from ``ray.wait`` works with a generator.

``ray.wait`` can mix up regular Ray object references and a generator as inputs. In this case, the application should handle
the output inside a ``ready`` list accordingly. Here's an example of mixing up Ray object references and generators.

.. literalinclude:: doc_code/streaming_generator.py
    :language: python
    :start-after: __streaming_generator_wait_complex_start__
    :end-before: __streaming_generator_wait_complex_end__

Thread Safety
-------------
``StreamingObjectRefGenerator`` object is not thread-safe.

Limitation
----------
There following features are not supported from Ray generators.

- ``throw``, ``send``, and ``close`` APIs are not supported.
- ``return`` statement from a generator is not supported.
- Passing an ``StreamingObjectRefGenerator`` to another task or actor is not allowed.
