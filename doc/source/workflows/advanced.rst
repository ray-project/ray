Advanced Topics
===============

Inplace Execution
-----------------

When executing a workflow task inside another workflow task, it is usually executed in another Ray worker process. This is good for resource and performance isolation, but at the cost of lower efficiency due to non-locality, scheduling and data transfer.

For example, this recursive workflow calculates the exponent. We write it with workflow so that we can recover from any task. However, it is really inefficient to scheduling each task in a different worker.

.. code-block:: python
    :caption: Workflow without inplace execution:

    import ray
    from ray import workflow

    @ray.remote
    def exp_remote(k, n):
        if n == 0:
            return k
        return workflow.continuation(exp_remote.bind(2 * k, n - 1))

We could optimize it with inplace option:

.. code-block:: python
    :caption: Workflow with inplace execution:

    import ray
    from ray import workflow

    @ray.remote
    def exp_inplace(k, n):
        if n == 0:
            return k
        return workflow.continuation(
            exp_inplace.options(**workflow.options(allow_inplace=True)).bind(2 * k, n - 1))

    assert workflow.create(exp_inplace.bind(3, 7)).run() == 3 * 2 ** 7


With ``allow_inplace=True``, the task that called ``.bind()`` executes in the function. Ray options are ignored because they are used for remote execution. Also, you cannot retrieve the output of an inplace task using ``workflow.get_output()`` before it finishes execution.

Inplace is also useful when you need to pass something that is only valid in the current process/physical machine to another task. For example:

.. code-block:: python

    @ray.remote
    def foo():
        x = "<something that is only valid in the current process>"
        return workflow.continuation(bar.options(**workflow.options(allow_inplace=True)).bind(x))


Wait for Partial Results
------------------------

By default, a workflow task will only execute after the completion of all of its dependencies. This blocking behavior prevents certain types of workflows from being expressed (e.g., wait for two of the three tasks to finish).

Analogous to ``ray.wait()``, in Ray Workflow we have ``workflow.wait(*tasks: List[Workflow[T]], num_returns: int = 1, timeout: float = None) -> (List[T], List[Workflow[T])``. Calling `workflow.wait` would generate a logical task . The output of the logical task is a tuple of ready workflow results, and workflow results that have not yet been computed. For example, you can use it to print out workflow results as they are computed in the following dynamic workflow:

.. code-block:: python

    @ray.remote
    def do_task(i):
       time.sleep(random.random())
       return "task {}".format(i)

    @ray.remote
    def report_results(wait_result: Tuple[List[str], List[Workflow[str]]]):
        ready, remaining = wait_result
        for result in ready:
            print("Completed", result)
        if not remaining:
            return "All done"
        else:
            return workflow.continuation(report_results.bind(workflow.wait(remaining)))

    tasks = [do_task.bind(i) for i in range(100)]
    report_results.bind(workflow.wait(tasks)).run()


Workflow task Checkpointing
---------------------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing. However, checkpointing could be time consuming, especially when you have large inputs and outputs for workflow tasks. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.


We control the checkpoints by specify the checkpoint options like this:

.. code-block:: python

    data = read_data.options(**workflow.options(checkpoint=False)).bind(10)

This example skips checkpointing the output of ``read_data``. During recovery, ``read_data`` would be executed again if recovery requires its output.

By default, we have ``checkpoint=True`` if not specified.

If the output of a task is another task (i.e. dynamic workflows), we skips checkpointing the entire task.

