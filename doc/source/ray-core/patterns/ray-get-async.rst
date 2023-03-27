Anti-pattern: Calling ray.get in coroutines blocks the event loop
=================================================================

**TLDR:** Avoid calling :func:`ray.get() <ray.get>` in coroutines since it will block the event loop; use ``await`` to get Ray objects.

``ray.get()`` is a blocking call and it will block the entire event loop when it's used inside coroutines.