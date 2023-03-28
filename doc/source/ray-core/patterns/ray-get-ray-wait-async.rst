Anti-pattern: Calling ray.get and ray.wait in coroutines blocks the event loop
==============================================================================

**TLDR:** Avoid calling :func:`ray.get() <ray.get>` and :func:`ray.wait() <ray.wait>` in coroutines since they will block the event loop;
use ``await`` to get or wait Ray objects.

``ray.get()`` and ``ray.wait()`` are blocking calls and they will block the entire event loop when they are used inside coroutines.
The better way in this case is to use ``await`` as ``ObjectRef`` is awaitable:

.. list-table::

    * - Sync
      - Async
    * - ray.get(obj_ref)
      - await obj_ref
    * - ray.get([obj_ref_1, obj_ref_2])
      - await asyncio.gather(obj_ref_1, obj_ref_2)
    * - ray.wait([obj_ref_1, obj_ref_2], num_returns=1)
      - await asyncio.wait([obj_ref_1, obj_ref_2], return_when=asyncio.FIRST_COMPLETED)

.. note::

    You can also run the blocking ray.get and ray.wait in coroutines using ``asyncio.to_thread()`` or ``loop.run_in_executor()``
    if ``ObjectRef`` as awaitable doesn't work for your case (e.g., you want to use fetch_local=False in ray.wait).

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_ray_get_ray_wait_async.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_ray_get_ray_wait_async.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__
