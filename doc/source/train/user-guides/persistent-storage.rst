Configuring Persistent Storage
==============================

.. _persistent-storage-guide:

.. _train-log-dir:

A Ray Train run produces results, checkpoints, and other artifacts.
These can be configured to be saved in a persistent storage location.

By default, these files are saved in a local directory under ``~/ray_results``.
This is sufficient for single-node setups or distributed training without saving
model checkpoints or artifacts, but some form of external persistent storage such as
cloud storage (e.g., S3, GCS) or NFS (e.g., AWS EFS, Google Filestore) is
highly recommended when doing multi-node training.

Here are some benefits of setting up persistent storage:

- **Checkpointing and fault tolerance**: Saving checkpoints to a persistent storage location
  allows you to resume training from the last checkpoint in case of a node failure.
- **Post-experiment analysis**: A consolidated location storing data from all trials is useful for post-experiment analysis
  such as accessing the best checkpoints and hyperparameter configs after the cluster has already been terminated.
- **Bridge with downstream serving/batch inference tasks**: With a configured storage, you can easily access the models
  and artifacts to share them with others or use them in downstream tasks.


Cloud storage (AWS S3, Google Cloud Storage)
--------------------------------------------

.. tip::

    Cloud storage is the recommended persistent storage option.

If all nodes in a Ray cluster have access to cloud storage, then all outputs can be uploaded to a shared cloud bucket.

Use cloud storage by specifying a bucket URI as the ``storage_path``:

.. code-block:: python

    from ray import train
    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(
        ...,
        run_config=train.RunConfig(
            storage_path="s3://bucket-name/sub-path/",
            name="experiment_name",
        )
    )


In this example, all files are uploaded to shared storage at ``s3://bucket-name/sub-path/experiment_name`` for further processing.


Network filesystem (NFS)
------------------------

If all Ray nodes have access to a network filesystem, e.g. AWS EFS or Google Cloud Filestore,
then all outputs can be saved to this shared filesystem.

Use NFS by specifying the mount path as the ``storage_path``:

.. code-block:: python

    from ray import train
    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(
        ...,
        run_config=train.RunConfig(
            storage_path="/mnt/cluster_storage",
            name="experiment_name",
        )
    )

In this example, all files are at ``/mnt/cluster_storage/experiment_name`` for further processing.


Local storage
-------------

Single-node cluster
~~~~~~~~~~~~~~~~~~~

If you're just running an experiment on a single node (e.g., on a laptop), Ray Train will use the
local filesystem as the storage location for checkpoints and other artifacts.
Results are saved to ``~/ray_results`` in a sub-directory with a unique auto-generated name by default,
unless you customize this with ``storage_path`` and ``name`` in :class:`~ray.train.RunConfig`.


.. code-block:: python

    from ray import train
    from ray.train.torch import TorchTrainer

    trainer = TorchTrainer(
        ...,
        run_config=train.RunConfig(
            storage_path="/tmp/custom/storage/path",
            name="experiment_name",
        )
    )


In this example, all experiment results can found locally at ``/tmp/custom/storage/path/experiment_name`` for further processing.


Multi-node cluster
~~~~~~~~~~~~~~~~~~

.. warning::

    When running on multiple nodes, using the local filesystem of the head node as the persistent storage location is no longer supported in most cases.

    If you save checkpoints with :meth:`ray.train.report(..., checkpoint=...) <ray.train.report>`
    and run on a multi-node cluster, Ray Train will raise an error if NFS or cloud storage is not setup.
    This is because Ray Train expects all workers to be able to write the checkpoint to
    the same persistent storage location.

    If your training loop does not save checkpoints, the reported metrics will still
    be aggregated to the local storage path on the head node.

    See `this issue <https://github.com/ray-project/ray/issues/37177>`_ for more information.


Advanced Configuration
----------------------

Setting the intermediate local directory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a ``storage_path`` is specified, 
By default, Ray Train will save intermediate files to a local directory under ``~/ray_results``.