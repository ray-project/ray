A Guide To Using Checkpoints
============================

.. _tune-checkpoint-syncing:

Checkpointing and synchronization
---------------------------------

When running a hyperparameter search, Tune can automatically and periodically save/checkpoint your model.
This allows you to:

* save intermediate models throughout training
* use pre-emptible machines (by automatically restoring from last checkpoint)
* Pausing trials when using Trial Schedulers such as HyperBand and PBT.

Tune stores checkpoints on the node where the trials are executed. If you are training on more than one node,
this means that some trial checkpoints may be on the head node and others are not.

When trials are restored (e.g. after a failure or when the experiment was paused), they may be scheduled on
different nodes, but still would need access to the latest checkpoint. To make sure this works, Ray Tune
comes with facilities to synchronize trial checkpoints between nodes.

Generally we consider three cases:

1. When using a shared directory (e.g. via NFS)
2. When using cloud storage (e.g. S3 or GS)
3. When using neither

The default option here is 3, which will be automatically used if nothing else is configured.

Using a shared directory
~~~~~~~~~~~~~~~~~~~~~~~~
If all Ray nodes have access to a shared filesystem, e.g. via NFS, they can all write to this directory.
In this case, we don't need any synchronization at all, as it is implicitly done by the operating system.

For this case, we only need to tell Ray Tune not to do any syncing at all (as syncing is the default):

.. code-block:: python

    from ray import tune

    tune.run(
        trainable,
        name="experiment_name",
        local_dir="/path/to/shared/storage/",
        sync_config=tune.SyncConfig(
            syncer=None  # Disable syncing
        )
    )

Note that the driver (on the head node) will have access to all checkpoints locally (in the
shared directory) for further processing.


.. _tune-cloud-checkpointing:

Using cloud storage
~~~~~~~~~~~~~~~~~~~
If all nodes have access to cloud storage, e.g. S3 or GS, the remote trials can automatically synchronize their
checkpoints. For the filesystem, we end up with a similar situation as in the first case,
only that the consolidated directory including all logs and checkpoints lives on cloud storage.

This approach is especially useful when training a large number of distributed trials,
as logs and checkpoints are otherwise synchronized via SSH, which quickly can become a performance bottleneck.

For this case, we tell Ray Tune to use an ``upload_dir`` to store checkpoints at.
This will automatically store both the experiment state and the trial checkpoints at that directory:

.. code-block:: python

    from ray import tune

    tune.run(
        trainable,
        name="experiment_name",
        sync_config=tune.SyncConfig(
            upload_dir="s3://bucket-name/sub-path/"
        )
    )

We don't have to provide a ``syncer`` here as it will be automatically detected. However, you can provide
a string if you want to use a custom command:

.. code-block:: python

    from ray import tune

    tune.run(
        trainable,
        name="experiment_name",
        sync_config=tune.SyncConfig(
            upload_dir="s3://bucket-name/sub-path/",
            syncer="aws s3 sync {source} {target}",  # Custom sync command
        )
    )

If a string is provided, then it must include replacement fields ``{source}`` and ``{target}``,
as demonstrated in the example above.

The consolidated data will live be available in the cloud bucket. This means that the driver
(on the head node) will not have access to all checkpoints locally. If you want to process
e.g. the best checkpoint further, you will first have to fetch it from the cloud storage.


Default syncing (no shared/cloud storage)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you're using neither a shared filesystem nor cloud storage, Ray Tune will resort to the
default syncing mechanisms, which utilizes ``rsync`` (via SSH) to synchronize checkpoints across
nodes.

Please note that this approach is likely the least efficient one - you should always try to use
shared or cloud storage if possible when training on a multi node cluster.

For the syncing to work, the head node must be able to SSH into the worker nodes. If you are using
the Ray cluster launcher this is usually the case (note that Kubernetes is an exception, but
:ref:`see here for more details <tune-kubernetes>`).

If you don't provide a ``tune.SyncConfig`` at all, rsync-based syncing will be used.

If you want to customize syncing behavior, you can again specify a custom sync template:

.. code-block:: python

    from ray import tune

    tune.run(
        trainable,
        name="experiment_name",
        sync_config=tune.SyncConfig(
            # Do not specify an upload dir here
            syncer="rsync -savz -e "ssh -i ssh_key.pem" {source} {target}",  # Custom sync command
        )
    )


Alternatively, a function can be provided with the following signature:

.. code-block:: python

    def custom_sync_func(source, target):
        sync_cmd = "rsync {source} {target}".format(
            source=source,
            target=target)
        sync_process = subprocess.Popen(sync_cmd, shell=True)
        sync_process.wait()

    tune.run(
        trainable,
        name="experiment_name",
        sync_config=tune.SyncConfig(
            syncer=custom_sync_func,
            sync_period=60  # Synchronize more often
        )
    )

When syncing results back to the driver, the source would be a path similar to
``ubuntu@192.0.0.1:/home/ubuntu/ray_results/trial1``, and the target would be a local path.

Note that we adjusted the sync period in the example above. Setting this to a lower number will pull
checkpoints from remote nodes more often. This will lead to more robust trial recovery,
but it will also lead to more synchronization overhead (as SSH is usually slow).

As in the first case, the driver (on the head node) will have access to all checkpoints locally
for further processing.

Checkpointing examples
----------------------

Let's cover how to configure your checkpoints storage location, checkpointing frequency, and how to resume from a previous run.

A simple (cloud) checkpointing example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cloud storage-backed Tune checkpointing is the recommended best practice for both performance and reliability reasons.
It also enables checkpointing if using Ray on Kubernetes, which does not work out of the box with rsync-based sync,
which relies on SSH. If you'd rather checkpoint locally or use rsync based checkpointing, see :ref:`here <rsync-checkpointing>`.

Prerequisites to use cloud checkpointing in Ray Tune for the example below:

Your ``my_trainable`` is either a:

1. **Model with an existing Ray integration**

  * XGBoost (:ref:`example <xgboost-ray-tuning>`)
  * Pytorch (:doc:`example </tune/examples/tune-pytorch-cifar>`)
  * Pytorch Lightning (:ref:`example <pytorch-lightning-tune>`)
  * Tensorflow/Keras (:doc:`example </tune/examples/tune_mnist_keras>`)
  * LightGBM (:ref:`example <lightgbm-ray-tuning>`)

2. **Custom training function**

  * All this means is that your function has to expose a ``checkpoint_dir`` argument in the function signature,
    and call ``tune.checkpoint_dir``. See :doc:`this example </tune/examples/includes/custom_func_checkpointing>`,
    it's quite simple to do.

Let's assume for this example you're running this script from your laptop, and connecting to your remote Ray cluster
via ``ray.init()``, making your script on your laptop the "driver".

.. code-block:: python

    import ray
    from ray import tune
    from your_module import my_trainable

    ray.init(address="<cluster-IP>:<port>")  # set `address=None` to train on laptop

    # configure how checkpoints are sync'd to the scheduler/sampler
    # we recommend cloud storage checkpointing as it survives the cluster when
    # instances are terminated, and has better performance
    sync_config = tune.syncConfig(
        upload_dir="s3://my-checkpoints-bucket/path/",  # requires AWS credentials
    )

    # this starts the run!
    tune.run(
        my_trainable,

        # name of your experiment
        name="my-tune-exp",

        # a directory where results are stored before being
        # sync'd to head node/cloud storage
        local_dir="/tmp/mypath",

        # see above! we will sync our checkpoints to S3 directory
        sync_config=sync_config,

        # we'll keep the best five checkpoints at all times
        # checkpoints (by AUC score, reported by the trainable, descending)
        checkpoint_score_attr="max-auc",
        keep_checkpoints_num=5,

        # a very useful trick! this will resume from the last run specified by
        # sync_config (if one exists), otherwise it will start a new tuning run
        resume="AUTO",
    )

In this example, checkpoints will be saved:

* **Locally**: not saved! Nothing will be sync'd to the driver (your laptop) automatically (because cloud syncing is enabled)
* **S3**: ``s3://my-checkpoints-bucket/path/my-tune-exp/<trial_name>/checkpoint_<step>``
* **On head node**: ``~/ray-results/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials done on that node)
* **On workers nodes**: ``~/ray-results/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials done on that node)

If your run stopped for any reason (finished, errored, user CTRL+C), you can restart it any time by running the script above again -- note with ``resume="AUTO"``, it will detect the previous run so long as the ``sync_config`` points to the same location.

If, however, you prefer not to use ``resume="AUTO"`` (or are on an older version of Ray) you can resume manaully:

.. code-block:: python

    # Restored previous trial from the given checkpoint
    tune.run(
        # our same trainable as before
        my_trainable,

        # The name can be different from your original name
        name="my-tune-exp-restart",

        # our same config as above!
        restore=sync_config,
    )

.. _rsync-checkpointing:

A simple local/rsync checkpointing example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Local or rsync checkpointing can be a good option if:

1. You want to tune on a single laptop Ray cluster
2. You aren't using Ray on Kubernetes (rsync doesn't work with Ray on Kubernetes)
3. You don't want to use S3

Let's take a look at an example:

.. code-block:: python

    import ray
    from ray import tune
    from your_module import my_trainable

    ray.init(address="<cluster-IP>:<port>")  # set `address=None` to train on laptop

    # configure how checkpoints are sync'd to the scheduler/sampler
    sync_config = tune.syncConfig()  # the default mode is to use use rsync

    # this starts the run!
    tune.run(
        my_trainable,

        # name of your experiment
        name="my-tune-exp",

        # a directory where results are stored before being
        # sync'd to head node/cloud storage
        local_dir="/tmp/mypath",

        # sync our checkpoints via rsync
        # you don't have to pass an empty sync config - but we
        # do it here for clarity and comparison
        sync_config=sync_config,

        # we'll keep the best five checkpoints at all times
        # checkpoints (by AUC score, reported by the trainable, descending)
        checkpoint_score_attr="max-auc",
        keep_checkpoints_num=5,

        # a very useful trick! this will resume from the last run specified by
        # sync_config (if one exists), otherwise it will start a new tuning run
        resume="AUTO",
    )

.. _tune-distributed-checkpointing:

Distributed Checkpointing
~~~~~~~~~~~~~~~~~~~~~~~~~

On a multinode cluster, Tune automatically creates a copy of all trial checkpoints on the head node.
This requires the Ray cluster to be started with the :ref:`cluster launcher <cluster-cloud>` and also
requires rsync to be installed.

Note that you must use the ``tune.checkpoint_dir`` API to trigger syncing
(or use a model type with a built-in Ray Tune integration as described here).
See :doc:`/tune/examples/includes/custom_func_checkpointing` for an example.

If you are running Ray Tune on Kubernetes, you should usually use a
:ref:`cloud checkpointing <tune-sync-config>` or a shared filesystem for checkpoint sharing.
Please :ref:`see here for best practices for running Tune on Kubernetes <tune-kubernetes>`.

If you do not use the cluster launcher, you should set up a NFS or global file system and
disable cross-node syncing:

.. code-block:: python

    sync_config = tune.SyncConfig(syncer=None)
    tune.run(func, sync_config=sync_config)
