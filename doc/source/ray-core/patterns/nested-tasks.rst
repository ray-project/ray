.. _task-pattern-nested-tasks:

Pattern: Using nested tasks to achieve nested parallelism
=========================================================

In this pattern, a remote task can dynamically call other remote tasks (including itself) for nested parallelism.
This is useful when sub-tasks can be parallelized.

Keepy in mind that nested tasks come with their own cost: extra worker processes, scheduling overhead, bookkeeping overhead, etc
so make sure your nested tasks do significant work to hide the overhead. See :doc:`too-fine-grained-tasks` for more details.
As a tip, you should always benchmark the nested tasks version of the code and make sure you get the speedup instead of slowdown!

Example use case
----------------

You want to quick-sort a large list of numbers.
By making the recursive calls distributed, we can sort the list distributedly and parallelly.

.. figure:: ../images/tree-of-tasks.svg

    Tree of tasks


Code example
------------

.. literalinclude:: ../doc_code/pattern_nested_tasks.py
    :language: python
    :start-after: __pattern_start__
    :end-before: __pattern_end__

We call :ref:`ray.get() <ray-get-ref>` after both ``quick_sort_distributed`` function invocations take place.
This allows you to maximize parallelism in the workload. See :doc:`ray-get-loop` for more details.

Notice in the execution times above that with smaller and finer tasks, the non-distributed version is faster; however, as the task execution
time increases, that is the task with larger list takes longer, the distributed version is faster.
