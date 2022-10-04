Pattern: Using resources to limit the number of concurrently running tasks
==========================================================================

In this pattern, we use :ref:`resources <resource-requirements>` to limit the number of concurrently running tasks.

Running too many tasks at the same time might overload the cluster and cause issues like OOM.
If that is the case, we can reduce the number of concurrently running tasks by increasing the amount of resources required by those tasks.
This works because Ray makes sure that the sum of the resource requirements of all of the concurrently running tasks on a given node cannot exceed the node's total resources.

Example use case
----------------

You have a data processing workload that processes each input file independently using Ray :ref:`remote functions <ray-remote-functions>`.
Since each task needs to load the input data into heap memory and do the processing, running too many of them can cause OOM.
In this case, you can use the ``memory`` resource to limit the number of concurrently running tasks (use other resources like ``num_cpus`` can achieve the same goal as well).


Code example
------------

.. literalinclude:: ../doc_code/limit_running_tasks.py
    :language: python
