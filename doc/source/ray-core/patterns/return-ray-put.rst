Anti-pattern: Returning ray.put() ObjectRefs from a task harms performance and fault tolerance
==============================================================================================

**TLDR:** Avoid calling :ref:`ray.put() <ray-put-ref>` on task return values and returning the resulting ObjectRefs.
Instead, return these values directly if possible.

Returning ray.put() ObjectRefs are considered anti-patterns for the following reasons:

- It disallows inlining small return values: Ray has a performance optimization to return small (<= 100KB) values inline directly to the caller, avoiding going through the distributed object store.
  On the other hand, ``ray.put()`` will unconditionally store the value to the object store which makes the optimization for small return values impossible.
- Returning ObjectRefs involves extra distributed reference counting protocol which is slower than returning the values directly.
- It's less fault tolerant: the worker process that calls ``ray.put()`` is the "owner" of the returned ``ObjectRef`` and the return value fate shares with the owner. If the worker process dies, the return value is lost.
  In contrast, the caller process (often the driver) is the owner of the return value if it's returned directly.

Code example
------------

If you want to return a single value regardless if it's small or large, you should return it directly.

.. literalinclude:: ../doc_code/anti_pattern_return_ray_put.py
    :language: python
    :start-after: __return_single_value_start__
    :end-before: __return_single_value_end__

If you want to return multiple values and you know the number of returns before calling the task, you should use the :ref:`num_returns <ray-task-returns>` option.

.. literalinclude:: ../doc_code/anti_pattern_return_ray_put.py
    :language: python
    :start-after: __return_static_multi_values_start__
    :end-before: __return_static_multi_values_end__

If you don't know the number of returns before calling the task, you should use the :ref:`dynamic generator <dynamic-generators>` pattern if possible.

.. literalinclude:: ../doc_code/anti_pattern_return_ray_put.py
    :language: python
    :start-after: __return_dynamic_multi_values_start__
    :end-before: __return_dynamic_multi_values_end__

.. note::

  Currently actor tasks don't support dynamic returns so you have to use ``ray.put()`` to store each return value and return a list of ObjectRefs in this case.
