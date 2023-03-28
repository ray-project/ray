.. _core-patterns-limit-running-tasks:

Pattern: Using resources to limit the number of concurrently running tasks
==========================================================================

In this pattern, we use :ref:`resources <resource-requirements>` to limit the number of concurrently running tasks.

By default, Ray tasks require 1 CPU each and Ray actors require 0 CPU each, so the scheduler limits task concurrency to the available CPUs and actor concurrency to infinite.
Tasks that use more than 1 CPU (e.g., via mutlithreading) may experience slowdown due to interference from concurrent ones, but otherwise are safe to run.

However, tasks or actors that use more than their proportionate share of memory may overload a node and cause issues like OOM.
If that is the case, we can reduce the number of concurrently running tasks or actors on each node by increasing the amount of resources requested by them.
This works because Ray makes sure that the sum of the resource requirements of all of the concurrently running tasks and actors on a given node does not exceed the node's total resources.

.. note::

   For actor tasks, the number of running actors limits the number of concurrently running actor tasks we can have.

Example use case
----------------

You have a data processing workload that processes each input file independently using Ray :ref:`remote functions <ray-remote-functions>`.
Since each task needs to load the input data into heap memory and do the processing, running too many of them can cause OOM.
In this case, you can use the ``memory`` resource to limit the number of concurrently running tasks (usage of other resources like ``num_cpus`` can achieve the same goal as well).
Note that similar to ``num_cpus``, the ``memory`` resource requirement is *logical*, meaning that Ray will not enforce the physical memory usage of each task if it exceeds this amount.

Code example
------------

**Without limit:**

.. literalinclude:: ../doc_code/limit_running_tasks.py
    :language: python
    :start-after: __without_limit_start__
    :end-before: __without_limit_end__

**With limit:**

.. literalinclude:: ../doc_code/limit_running_tasks.py
    :language: python
    :start-after: __with_limit_start__
    :end-before: __with_limit_end__
