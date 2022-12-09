A Guide To Using Checkpoints
============================

.. _tune-two-types-of-ckpt:

Two different types of Tune checkpoints
---------------------------------------

There are mainly two types of checkpoints that Tune maintains: experiment-level checkpoints and trial-level
checkpoints.

Experiment Checkpoints
~~~~~~~~~~~~~~~~~~~~~~

Experiment-level checkpoints save the experiment state. This includes the state of the searcher,
the list of trials and their statuses (e.g. PENDING, RUNNING, TERMINATED, ERROR), and
metadata pertaining to each trial (e.g. hyperparameter configuration, trial logdir, etc).

The experiment-level checkpoint is periodically saved by the driver on the head node.
By default, the frequency at which it is saved is automatically
adjusted so that at most 5% of the time is spent saving experiment checkpoints,
and the remaining time is used for handling training results and scheduling.
This time can also be adjusted with the
:ref:`TUNE_GLOBAL_CHECKPOINT_S environment variable <tune-env-vars>`.

The purpose of the experiment checkpoint is to maintain a global state from which the whole Ray Tune experiment
can be resumed from if it is interrupted or failed.
It is also used to load tuning results after a Ray Tune experiment has finished.

Trial Checkpoints
~~~~~~~~~~~~~~~~~

Trial-level checkpoints capture the per-trial state. They are saved by the :ref:`trainable <tune-key-concepts-trainables>` itself.
This often includes the model and optimizer states. Here are a few uses of trial checkpoints:

- If the trial is interrupted for some reason (e.g. on spot instances), it can be resumed from the
  last state. No training time is lost.
- Some searchers/schedulers pause trials to free resources so that other trials can train in
  the meantime. This only makes sense if the trials can then continue training from the latest state.
- The checkpoint can be later used for other downstream tasks like batch inference.

Everything that is saved by ``session.report()`` (if using the Function API) or
``Trainable.save_checkpoint`` (if using the Class API) is a **trial-level checkpoint.**
See :ref:`checkpointing with the Function API <tune-function-checkpointing>` and
:ref:`checkpointing with the Class API <tune-trainable-save-restore>`
for examples of saving and loading trial-level checkpoints.

.. _tune-checkpoint-syncing:

Synchronizing checkpoints in a multi-node Tune experiment
---------------------------------------------------------

Experiment checkpoints are stored on the driver node (the head node),
and trial checkpoints are stored on the node where the trials are executed.
If you are training on more than one node, this means that some trial checkpoints may
be on the head node and others are not.

When trials are restored (e.g. after a failure or when the experiment was paused), they may be scheduled on
different nodes, but still would need access to the latest checkpoint. To make sure this works, Ray Tune
comes with facilities to synchronize checkpoints between nodes, so that there is a
consolidated directory that experiment and trial checkpoints can be accessed from.

Generally we consider three cases:

1. When using a shared directory (e.g. via NFS)
2. When using cloud storage (e.g. S3 or GCS)
3. When using neither

The default option here is 3, which will be automatically used if nothing else is configured.

Using a shared directory
~~~~~~~~~~~~~~~~~~~~~~~~
If all Ray nodes have access to a shared filesystem, e.g. via NFS, they can all write to this directory.
In this case, we don't need any synchronization at all, as it is implicitly done by the operating system.

For this case, we only need to tell Ray Tune not to do any syncing at all (as syncing is the default):

.. code-block:: python

    from ray import air, tune

    tuner = tune.Tuner(
        trainable,
        run_config=air.RunConfig(
            name="experiment_name",
            local_dir="/path/to/shared/storage/",
            sync_config=tune.SyncConfig(
                syncer=None  # Disable syncing
            )
        )
    )
    tuner.fit()

Note that the driver (on the head node) will have access to all checkpoints locally (in the
shared directory) for further processing.


.. _tune-cloud-checkpointing:

Using cloud storage
~~~~~~~~~~~~~~~~~~~
Using cloud storage is similar to using a shared filesystem: the only difference is
that the consolidated directory (including all logs and checkpoints) lives in the cloud storage.

If all nodes have access to cloud storage, e.g. S3 or GCS, remote trials can directly upload their
trial checkpoints to the cloud storage.
This approach is especially useful when training a large number of distributed trials,
where the default syncing behavior (see below) with many worker nodes can introduce significant overhead.

For this case, we tell Ray Tune to store experiment and trial checkpoints at a remote ``upload_dir``:

.. code-block:: python

    from ray import tune
    from ray.air.config import RunConfig

    tuner = tune.Tuner(
        trainable,
        run_config=RunConfig(
            name="experiment_name",
            sync_config=tune.SyncConfig(
                upload_dir="s3://bucket-name/sub-path/",
                syncer="auto",
            )
        )
    )
    tuner.fit()

``syncer="auto"`` automatically configures a default syncer that uses pyarrow to
perform syncing with the specified cloud ``upload_dir``.
The ``syncer`` config can also take in a custom :class:`Syncer <ray.tune.syncer.Syncer>`
if you want to implement custom syncing logic.
See :ref:`tune-cloud-syncing` and :ref:`tune-cloud-syncing-command-line-example`
for more details and examples.

The consolidated experiment data will be available in the cloud bucket at ``s3://bucket-name/sub-path/experiment_name``.
This means that the driver (on the head node) will not have access to all checkpoints locally. If you want to process
e.g. the best checkpoint further, you will first have to fetch it from the cloud storage.

Experiment restoration should also be done using the experiment directory at the cloud storage
URI, rather than the local experiment directory on the head node. See :ref:`here for an example <tune-syncing-restore-from-uri>`.


Default syncing (no shared/cloud storage)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If you're using neither a shared filesystem nor cloud storage, Ray Tune will resort to the
default syncing mechanism, which uses the Ray object store to send the contents of the trial directory
(containing checkpoints) from worker nodes to the head node.

.. note::

    If you don't provide a ``tune.SyncConfig`` at all, this is the method of syncing that will be used.

By default, the driver will pull a trial's directory to the head node whenever that trial
has finished saving a checkpoint. This can be configured by ``sync_on_checkpoint`` and
``sync_period`` in :class:`SyncConfig <ray.tune.syncer.SyncConfig>`:

.. code-block:: python

    from ray import tune
    from ray.air.config import RunConfig

    tuner = tune.Tuner(
        trainable,
        run_config=RunConfig(
            name="experiment_name",
            sync_config=tune.SyncConfig(
                syncer="auto",
                # Sync approximately every minute rather than on every checkpoint
                sync_on_checkpoint=False,
                sync_period=60,
            )
        )
    )
    tuner.fit()

In the example above, we disabled forceful syncing on trial checkpoints and adjusted the sync period to 60 seconds.
Setting the sync period to a lower number will pull checkpoints from remote nodes more often.
This will lead to more robust trial recovery, but it will also lead to more synchronization overhead.

As in the first case, the driver (on the head node) will have access to all checkpoints locally
for further processing.

.. warning::
    Please note that this approach is likely the least efficient one - you should always try to use
    shared or cloud storage if possible when training on a multi-node cluster.


Checkpointing examples
----------------------

Let's cover how to configure your checkpoints storage location, checkpointing frequency, and how to resume from a previous run.

A simple cloud checkpointing example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tip::

    Cloud storage-backed Tune checkpointing is the recommended best practice for both performance and reliability reasons.

Let's assume for this example you're running this script from your laptop, and connecting to your remote Ray cluster
via ``ray.init(address="<cluster-IP>:<port>")``.

In the example below, ``my_trainable`` is a Tune :ref:`trainable <trainable-docs>`
that implements saving and loading checkpoints.

.. code-block:: python

    import ray
    from ray import air, tune
    from your_module import my_trainable

    ray.init(address="<cluster-IP>:<port>")

    # Configure how experiment data and checkpoints are sync'd
    # We recommend cloud storage checkpointing as it survives the cluster when
    # instances are terminated and has better performance
    sync_config = tune.SyncConfig(
        upload_dir="s3://my-checkpoints-bucket/path/",  # requires AWS credentials
    )

    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            # Name of your experiment
            name="my-tune-exp",
            # Directory where each node's results are stored before being
            # sync'd to cloud storage
            local_dir="/tmp/mypath",
            # See above! we will sync our checkpoints to S3 directory
            sync_config=sync_config,
            checkpoint_config=air.CheckpointConfig(
                # We'll keep the best five checkpoints at all times
                # checkpoints (by AUC score, reported by the trainable, descending)
                checkpoint_score_attribute="max-auc",
                num_to_keep=5,
            ),
        ),
    )
    # This starts the run!
    results = tuner.fit()

In this example, here's how checkpoints will be saved:

- **Locally on laptop**: Not saved here! Nothing will be sync'd to your laptop, since the experiment is being run on the remote cluster.
- On head node:
    - Experiment checkpoint: all checkpoint data stored at the experiment directory level (ex: ``/tmp/mypath/my-tune-exp/experiment-state-<date>.json``)
    - Trial checkpoints: ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)
- On worker nodes:
    - Experiment checkpoint: not stored on worker nodes!
    - Trial checkpoints: ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)
- S3:
    - Experiment checkpoint: all checkpoint data stored at the experiment directory level (ex: ``s3://my-checkpoints-bucket/path/my-tune-exp/experiment-state-<date>.json``)
    - Trial checkpoints: ``s3://my-checkpoints-bucket/path/my-tune-exp/<trial_name>/checkpoint_<step>`` (all trials)

.. _tune-syncing-restore-from-uri:

If this run stopped for any reason (ex: user CTRL+C, terminated due to out of memory issues),
you can resume it any time starting from the experiment checkpoint state saved in the cloud:

.. code-block:: python

    from ray import tune
    tuner = tune.Tuner.restore(
        "s3://my-checkpoints-bucket/path/my-tune-exp",
        resume_errored=True
    )
    tuner.fit()


There are a few options for restoring an experiment:
``resume_unfinished``, ``resume_errored`` and ``restart_errored``.
Please see the documentation of
:meth:`Tuner.restore() <ray.tune.tuner.Tuner.restore>` for more details.

.. _tune-default-syncing:

A simple example using default checkpoint syncing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, let's take a look at an example using default syncing behavior described above.

This time, we'll consider the case of running the Tune experiment directly on the head node of an existing
Ray cluster: ``ray.init()`` in the example below will automatically detect and connect to it.

.. code-block:: python

    import ray
    from ray import tune
    from your_module import my_trainable

    # Look for the existing cluster and connect to it
    ray.init()

    sync_config = tune.SyncConfig()

    # This starts the run!
    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            name="my-tune-exp",
            local_dir="/tmp/mypath",
            # Use the default syncing behavior
            # You don't have to pass an empty sync config - but we
            # do it here for clarity and comparison
            sync_config=sync_config,
            checkpoint_config=air.CheckpointConfig(
                checkpoint_score_attribute="max-auc",
                num_to_keep=5,
            ),
        )
    )

In this example, here's how checkpoints will be saved:

- On head node where we are running from:
    - Experiment checkpoint: all checkpoint data stored at the experiment directory level (ex: ``/tmp/mypath/my-tune-exp/experiment-state-<date>.json``)
    - Trial checkpoints: ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (all trials, since they have been synced to the head node)
- On worker nodes:
    - Experiment checkpoint: not stored on worker nodes!
    - Trial checkpoints: ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)

This experiment can be resumed from the head node:

.. code-block:: python

    from ray import tune
    tuner = tune.Tuner.restore(
        "/tmp/mypath/my-tune-exp",
        resume_errored=True
    )
    tuner.fit()