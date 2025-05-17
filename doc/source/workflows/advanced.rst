Advanced Topics
===============

.. warning::

  The experimental Ray Workflows library has been deprecated and will be removed in a
  future version of Ray.

Skipping Checkpoints
--------------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing. However, checkpointing could be time consuming, especially when you have large inputs and outputs for workflow tasks. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.

Checkpoints can be skipped by specifying ``checkpoint=False``:

.. testcode::

    import ray
    from ray import workflow

    @ray.remote
    def read_data(num: int):
        return [i for i in range(num)]

    data = read_data.options(**workflow.options(checkpoint=False)).bind(10)

This example skips checkpointing the output of ``read_data``. During recovery, ``read_data`` would be executed again if recovery requires its output.

If the output of a task is another task (i.e., for dynamic workflows), we skip checkpointing the entire task.
