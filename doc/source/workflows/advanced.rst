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


Wait for Partial Results
------------------------

By default, a workflow step will only execute after the completion of all of its dependencies. This blocking behavior prevents certain types of workflows from being expressed (e.g., wait for two of the three steps to finish).

Analogous to ``ray.wait()``, in Ray Workflow we have ``workflow.wait(*steps: List[Workflow[T]], num_returns: int = 1, timeout: float = None) -> (List[T], List[Workflow[T])``. Calling `workflow.wait` would generate a logical step . The output of the logical step is a tuple of ready workflow results, and workflow results that have not yet been computed. For example, you can use it to print out workflow results as they are computed in the following dynamic workflow:

.. code-block:: python

    @workflow.step
    def do_task(i):
       time.sleep(random.random())
       return "task {}".format(i)

    @workflow.step
    def report_results(wait_result: Tuple[List[str], List[Workflow[str]]]):
        ready, remaining = wait_result
        for result in ready:
            print("Completed", result)
        if not remaining:
            return "All done"
        else:
            return report_results.step(workflow.wait(remaining))

    tasks = [do_task.step(i) for i in range(100)]
    report_results.step(workflow.wait(tasks)).run()


Workflow Step Checkpointing
---------------------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing. However, checkpointing could be time consuming, especially when you have large inputs and outputs for workflow steps. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.


We control the checkpoints by specify the checkpoint options like this:

.. code-block:: python

    data = read_data.options(checkpoint=False).step(10)

This example skips checkpointing the output of ``read_data``. During recovery, ``read_data`` would be executed again if recovery requires its output.

By default, we have ``checkpoint=True`` if not specified.

If the output of a step is another step (i.e. dynamic workflows), we skips checkpointing the entire step.

