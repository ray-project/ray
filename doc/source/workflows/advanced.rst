Advanced Topics
===============

Skipping Checkpoints
--------------------

Ray Workflows provides strong fault tolerance and exactly-once execution semantics by checkpointing. However, checkpointing could be time consuming, especially when you have large inputs and outputs for workflow tasks. When exactly-once execution semantics is not required, you can skip some checkpoints to speed up your workflow.

Checkpoints can be skipped by specifying ``checkpoint=False``:

.. code-block:: python

    data = read_data.options(**workflow.options(checkpoint=False)).bind(10)

This example skips checkpointing the output of ``read_data``. During recovery, ``read_data`` would be executed again if recovery requires its output.

If the output of a task is another task (i.e., for dynamic workflows), we skip checkpointing the entire task.

Use Workflows with Ray Client
-----------------------------

Ray Workflows supports :ref:`Ray Client API <ray-client-ref>`, so you can submit workflows to a remote
Ray cluster. This requires starting the Ray cluster with the ``--storage=<storage_uri>`` option
for specifying the workflow storage.

To submit a workflow to a remote cluster, all you need is connect Ray to the cluster before
submitting a workflow. No code changes are required. For example:

.. code-block:: python

    import subprocess
    import ray
    from ray import workflow

    @ray.remote
    def hello(count):
        return ["hello world"] * count

    try:
        subprocess.check_call(
            ["ray", "start", "--head", "--ray-client-server-port=10001", "--storage=file:///tmp/ray/workflow_data"])
        ray.init("ray://127.0.0.1:10001")
        assert workflow.run(hello.bind(3)) == ["hello world"] * 3
    finally:
        subprocess.check_call(["ray", "stop"])


.. warning::

  Ray client support is still experimental and has some limitations. One known limitation is that
  workflows will not work properly with ObjectRefs as workflow task inputs. For example,
  ``workflow.run(task.bind(ray.put(123)))``.
