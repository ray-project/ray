Troubleshooting
===============

This page contains common issues and solutions for Compiled Graph execution.

Limitations
-----------

Compiled Graph is a new feature and has some limitations. Different from classic Ray Core:

- Invoking Compiled Graph
  - Only the process that compiles the Compiled Graph may call it.
  - A Compiled Graph has a maximum number of in-flight executions. Normally when using classic Ray
    Core, if there are not enough resources at the time of ``dag.execute()``, Ray will queue the
    tasks for later execution. Ray Compiled Graph currently does not support such queuing past their
    maximum capacity. Therefore, you may need to consume some results using ``ray.get()``
    before submitting more executions. As a stopgap,
    ``dag.execute()`` will throw a ``RayCgraphCapacityExceeded`` exception if the call takes too long.
    In the future, we plan to introduce better error handling and queuing for Compiled Graph.

- Compiled Graph Execution
  - Ideally, you should try not to execute other tasks on the actor while it is participating in a Compiled Graph.
    Compiled Graph tasks will be executed on a **background thread**. Any concurrent tasks
    submitted to the actor can still execute on the main thread, but you are responsible for
    synchronization with the Compiled Graph background thread.
  - For now, actors can only execute one Compiled Graph at a time. To execute a different Compiled Graph
    on the same actor, the current Compiled Graph must be torn down. See :ref:`troubleshoot-teardown` for more details.


- Passing and getting Compiled Graph results (``CompiledDAGRef``s)
  - Compiled Graph results cannot be passed to another task or actor. This restriction may be loosened
    in the future, but for now, it allows for better performance because the backend knows
    exactly where to push the results.
  - ``ray.get()`` can be called at most once on a ``CompiledDAGRef``. An exception will be raised if
    it is called twice on the same ``CompiledDAGRef``. This is because the underlying memory for
    the result may need to be reused for a future DAG execution; restricting ``ray.get()`` to once
    per reference simplifies the tracking of the memory buffers.
  - If the value returned by ``ray.get()`` is zero-copy deserialized, then subsequent executions
    of the same DAG will block until the value goes out of scope in Python. Thus, if you hold onto
    zero-copy deserialized values returned by ``ray.get()``, and you try to execute the DAG above
    its max concurrency, you may deadlock. This case will be detected in the future, but for now
    you will receive a ``RayChannelTimeoutError``. See :ref:`troubleshoot-numpy` for more details.

- Collective operations
  - Compiled Graph supports all-reduce collective operation now, but not yet other collective operations.
  
Keep an eye out for additional features in future Ray releases:
- Support better queuing of DAG inputs, to enable more concurrent executions of the same DAG.
- Support for more collective operations with NCCL.
- Support for multiple DAGs executing on the same actor.
- General performance improvements.

If you run into additional issues, or have other feedback or questions, please file an issue on GitHub.
For a full list of known issues, check the ``compiled-graph`` label on Ray GitHub.

.. _troubleshoot-numpy:
Returning NumPy arrays
----------------------
Ray zero-copy deserializes NumPy arrays when possible. If you execute compiled graph with a NumPy array output multiple times, 
you could possibly run into issues if a NumPy array output from a previous Compiled Graph execution isn't deleted before attempting to get the result 
of a following execution of the same Compiled Graph. This is because the NumPy array stays in the buffer of the Compiled Graph until you or Python delete it. 
It's recommended to explicitly delete the NumPy array as Python may not always garbage collect the NumPy array immediately as you may expect.

For example, the following code sample could result in a hang or RayChannelTimeoutError if the NumPy array isn't deleted:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __numpy_troubleshooting_start__
    :end-before: __numpy_troubleshooting_end__

In the preceding code snippet, Python may not garbage collect the NumPy array in `result` on each iteration of the loop. 
Therefore, you should explicitly delete the NumPy array before you try to get the result of subsequent Compiled Graph executions.

.. _troubleshoot-teardown:
Explicitly teardown before reusing the same actors
--------------------------------------------------
If you want to reuse the actors of a Compiled Graph, it's important to explicitly teardown the Compiled Graph before reusing the actors. 
Without explicitly tearing down the Compiled Graph, the resources created for actors in a Compiled Graph may have conflicts with further usage of those actors.

For example, in the following code, Python could delay garbage collection, which triggers the implicit teardown of the first Compiled Graph. This could lead to a segfault due to the resource conflicts mentioned:

.. literalinclude:: ../doc_code/cgraph_troubleshooting.py
    :language: python
    :start-after: __teardown_troubleshooting_start__
    :end-before: __teardown_troubleshooting_end__
