Anti-pattern: Returning ray.put() ObjectRefs from a task harms performance and fault tolerance
==============================================================================================

**TLDR:** Avoid calling :ref:`ray.put() <ray-put-ref>` on task return values and returning the resulting ObjectRefs;
returning these values directly via :ref:`static <ray-task-returns>` and :ref:`dynamic <dynamic-generators>` ``num_returns``.

Returning ray.put() ObjectRefs are considered anti-patterns for the following reasons:

- It disallows inlining small return values: Ray has a performance optimization to return small (<= 100KB) values inline directly to the owner, avoiding going through the distributed object store.
  On the other hand, ``ray.put()`` will unconditionally store the value to the object store which makes the optimization for small return values impossible.
- Returning ObjectRefs involves extra distributed reference counting protocol which is slower than returning the values directly.
- It's less fault tolerant: the worker process that calls ``ray.put()`` is the owner of the return value and the return value fates share with the owner meaning that if the worker process dies, the return value is lost.
  In contrast, the caller process (often to be the driver) is the owner of the return value if it's returned directly.
  Fate sharing with the caller process is better than fate sharing with the callee process given that it's the caller that uses the return value.

.. note::

  Currently actor tasks don't support dynamic returns so you have to use ``ray.put()`` to store each return value and return a list of ObjectRefs in this case.

Code example
------------

.. literalinclude:: ../doc_code/anti_pattern_return_ray_put.py
    :language: python
