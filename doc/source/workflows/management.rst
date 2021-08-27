Workflow Management
===================

Workflow IDs
------------
Each workflow has a unique ``workflow_id``. By default, when you call ``.run()`` or ``.run_async()``, a random id is generated. It is recommended you explicitly assign each workflow an id via ``.run(workflow_id="id")``.

If ``.run()`` is called with a previously used workflow id,  the workflow will be resumed, unless ``resume_existing=False``, in which case an error will be raised.

Workflow States
---------------
A workflow can be in one of several states:

=================== =======================================================================================
Status              Description
=================== =======================================================================================
RUNNING             The workflow is currently running in the cluster.
RUNNING             The workflow is currently running in the cluster.
FAILED              This workflow failed with an application error. It can be resumed from the failed step.
RESUMABLE           This workflow failed with a system error. It can be resumed from the failed step.
CANCELED            The workflow was canceled. Its result is unavailable, and it cannot be resumed.
SUCCESSFUL          The workflow has been executed successfully.
=================== =======================================================================================

Single workflow management APIs
-------------------------------

.. code-block:: python

    from ray import workflow

    # Get the status of a workflow.
    try:
        status = workflow.get_status(workflow_id="workflow_id")
        assert status in {
            "RUNNING", "RESUMABLE", "FAILED",
            "CANCELED", "SUCCESSFUL"}
    except ValueError:
        print("Workflow doesn't exist.")

    # Resume a workflow.
    print(workflow.resume(workflow_id="workflow_id")))
    # return is an ObjectRef which is the result of this workflow

    # Cancel a workflow.
    workflow.cancel(workflow_id="workflow_id")

Bulk workflow management APIs
-----------------------------

.. code-block:: python

    # List all running workflows.
    print(workflow.list_all("RUNNING"))
    # [("workflow_id_1", "RUNNING"), ("workflow_id_2", "RUNNING")]

    # List RUNNING and CANCELED workflows.
    print(workflow.list_all({"RUNNING", "CANCELED"}))
    # [("workflow_id_1", "RUNNING"), ("workflow_id_2", "CANCELED")]

    # List all workflows.
    print(workflow.list_all())
    # [("workflow_id_1", "RUNNING"), ("workflow_id_2", "CANCELED")]

    # Resume all resumable workflows. This won't include failed workflow
    print(workflow.resume_all())
    # [("workflow_id_1", ObjectRef), ("workflow_id_2", ObjectRef)]

    # To resume workflows including failed ones, use `include_failed=True`
    print(workflow.resume_all(include_failed=True))
    # [("workflow_id_1", ObjectRef), ("workflow_id_3", ObjectRef)]

Recurring workflows
-------------------

Ray workflows currently has no built-in job scheduler. You can however easily use any external job scheduler to connect to your Ray cluster via Ray client and trigger workflow runs.

Storage Configuration
---------------------
Workflows supports two types of storage backends out of the box:

*  Local file system: the data is stored locally. This is only for single node testing. It needs to be a NFS to work with multi-node clusters. To use local storage, specify ``workflow.init(storage="/path/to/storage_dir")``.
*  S3: Production users should use S3 as the storage backend. Enable S3 storage with ``workflow.init(storage="s3://bucket/path")``.

Additional storage backends can be written by subclassing the ``Storage`` class and passing a storage instance to ``workflow.init()`` [TODO: note that the Storage API is not currently stable].

Besides ``workflow.init()``, the storage URI can also be set via environment variable:

.. code-block:: python

    import os
    from ray import workflow

    # Option 1: pass the url through ``workflow.init``
    workflow.init("/local/path")

    # Option 2: set os environment variable RAY_WORKFLOW_STORAGE
    os.environ["RAY_WORKFLOW_STORAGE"] = "s3://bucket/path"
    workflow.init()

If left unspecified, ``/tmp/ray/workflow_data`` will be used for temporary storage. This default setting *will only work for single-node Ray clusters*.


Dependency Management
---------------------

**Note: This feature is not yet implemented.**

Ray logs the runtime environment (code and dependencies) of the workflow to storage at submission time. This ensures that the workflow can be resumed at a future time on a different Ray cluster.

You can also explicitly set the runtime environment for a particular step (e.g., specify conda environment, container image, etc.).

For virtual actors, the runtime environment of the actor can be upgraded via the virtual actor management API.
