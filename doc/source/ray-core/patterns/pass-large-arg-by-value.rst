.. _ray-pass-large-arg-by-value:

Anti-pattern: Passing the same large argument by value repeatedly harms performance
===================================================================================

**Summary:** Avoid passing the same large argument by value to multiple tasks, use :func:`ray.put() <ray.put>` and pass by reference instead.

When passing a large argument (>100KB) by value to a task,
Ray implicitly stores the argument in the object store and the worker process fetches the argument to the local object store from the caller's object store before running the task.
If you pass the same large argument to multiple tasks, Ray ends up storing multiple copies of the argument in the object store since Ray doesn't do deduplication.

Instead of passing the large argument by value to multiple tasks,
use ``ray.put()`` to store the argument to the object store once and get an ``ObjectRef``,
then pass the argument reference to tasks. This way, all tasks use the same copy of the argument, which is faster and uses less object store memory.

Code example
------------

**Anti-pattern:**

.. literalinclude:: ../doc_code/anti_pattern_pass_large_arg_by_value.py
    :language: python
    :start-after: __anti_pattern_start__
    :end-before: __anti_pattern_end__

**Better approach:**

.. literalinclude:: ../doc_code/anti_pattern_pass_large_arg_by_value.py
    :language: python
    :start-after: __better_approach_start__
    :end-before: __better_approach_end__
