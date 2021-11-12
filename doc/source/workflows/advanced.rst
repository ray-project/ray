Advanced Topics
===============

Inplace Execution
-----------------

When executing a workflow step inside another workflow step, it is usually executed in another Ray worker process. This is good for resource and performance isolation, but at the cost of lower efficiency due to non-locality, scheduling and data transfer.

For example, this recursive workflow calculates the exponent. We write it with workflow so that we can recover from any step. However, it is really inefficient to scheduling each step in a different worker.

.. code-block:: python
    :caption: Workflow without inplace execution:

    @workflow.step
    def exp_remote(k, n):
        if n == 0:
            return k
        return exp_remote.step(2 * k, n - 1)

We could optimize it with inplace option:

.. code-block:: python
    :caption: Workflow with inplace execution:

    def exp_inplace(k, n, worker_id=None):
        if n == 0:
            return k
        return exp_inplace.options(allow_inplace=True).step(
            2 * k, n - 1, worker_id)

With ``allow_inplace=True``, the step that called ``.step()`` executes in the function. Ray options are ignored because they are used for remote execution. Also, you cannot retrieve the output of an inplace step using ``workflow.get_output()`` before it finishes execution.

Inplace is also useful when you need to pass something that is only valid in the current process/physical machine to another step. For example:

.. code-block:: python
    @workflow.step
    def Foo():
        x = "<something that is only valid in the current process>"
        return Bar.options(allow_inplace=True).step(x)
