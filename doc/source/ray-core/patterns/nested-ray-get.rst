.. _nested-ray-get:

Anti-pattern: Calling ray.get on task arguments harms performance
=================================================================


**TLDR:** If possible, pass ``ObjectRefs`` as direct task arguments, instead of passing a list as the task argument and then calling :func:`ray.get() <ray.get>` inside the task.

When a task calls ``ray.get()``, it must block until the value of the ``ObjectRef`` is ready.
If all cores are already occupied, this situation can lead to a deadlock, as the task that produces the ``ObjectRef``'s value may need the caller task's resources in order to run.
To handle this issue, if the caller task would block in ``ray.get()``, Ray temporarily releases the caller's CPU resources to allow the pending task to run.
This behavior can harm performance and stability because the caller continues to use a process and memory to hold its stack while other tasks run.

Therefore, it is always better to pass ``ObjectRefs`` as direct arguments to a task and avoid calling ``ray.get`` inside of the task, if possible.

For example, in the following code, prefer the latter method of invoking the dependent task.

.. literalinclude:: ../doc_code/anti_pattern_nested_ray_get.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

Avoiding ``ray.get`` in nested tasks may not always be possible. Some valid reasons to call ``ray.get`` include:

- :doc:`nested-tasks`
- If the nested task has multiple ``ObjectRefs`` to ``ray.get``, and it wants to choose the order and number to get.
