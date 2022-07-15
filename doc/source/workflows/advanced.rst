Advanced Topics
===============

Workflow task Checkpointing
---------------------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing. However, checkpointing could be time consuming, especially when you have large inputs and outputs for workflow tasks. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.


We control the checkpoints by specify the checkpoint options like this:

.. code-block:: python

    data = read_data.options(**workflow.options(checkpoint=False)).bind(10)

This example skips checkpointing the output of ``read_data``. During recovery, ``read_data`` would be executed again if recovery requires its output.

By default, we have ``checkpoint=True`` if not specified.

If the output of a task is another task (i.e. dynamic workflows), we skips checkpointing the entire task.

