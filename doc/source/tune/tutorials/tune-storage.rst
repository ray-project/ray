.. _tune-storage-options:

How to Configure Storage Options for a Distributed Tune Experiment
==================================================================

When running Tune in a distributed setting, trials run on many different machines,
which means that experiment outputs such as model checkpoints will be spread all across the cluster.

Tune allows you to configure persistent storage options to enable following use cases in a distributed Ray cluster:

- **Trial-level fault tolerance**: When trials are restored (e.g. after a node failure or when the experiment was paused),
  they may be scheduled on different nodes, but still would need access to their latest checkpoint.
- **Experiment-level fault tolerance**: For an entire experiment to be restored (e.g. if the cluster crashes unexpectedly),
  Tune needs to be able to access the latest experiment state, along with all trial
  checkpoints to start from where the experiment left off.
- **Post-experiment analysis**: A consolidated location storing data from all trials is useful for post-experiment analysis
  such as accessing the best checkpoints and hyperparameter configs after the cluster has already been terminated.


Storage Options in Tune
-----------------------

Tune provides support for three scenarios:

1. When running Tune on a distributed cluster without any external persistent storage.
2. When using a network filesystem (NFS) mounted to all machines in the cluster.
3. When using cloud storage (e.g. AWS S3 or Google Cloud Storage) accessible by all machines in the cluster.

Situation (1) is the default scenario if a network filesystem or cloud storage are not provided.
In this scenario, we assume that we only have the local filesystems of each machine in the Ray cluster for storing experiment outputs.

.. note::

    Although we are considering distributed Tune experiments in this guide,
    a network filesystem or cloud storage can also be configured for single-node
    experiments. This can be useful to persist your experiment results in external storage
    if, for example, the instance you run your experiment on clears its local storage
    after termination.

.. seealso::

    See :class:`~ray.tune.syncer.SyncConfig` for the full set of configuration options as well as more details.


.. _tune-default-syncing:

Configure Tune without external persistent storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you're using neither a shared filesystem nor cloud storage, Ray Tune will resort to the
default mechanism of periodically synchronizing data saved on worker nodes to the head node.
**This treats the head node's local filesystem as the main storage location of the distributed Tune experiment.**

By default, workers will sync to the head node whenever a trial running on that workers
has finished saving a checkpoint. This can be configured by ``sync_on_checkpoint`` and
``sync_period`` in :class:`SyncConfig <ray.tune.syncer.SyncConfig>`:

.. code-block:: python
    :emphasize-lines: 9, 10, 11, 12, 13, 14

    from ray import tune
    from ray.air.config import RunConfig

    tuner = tune.Tuner(
        trainable,
        run_config=RunConfig(
            name="experiment_name",
            local_dir="~/ray_results",
            sync_config=tune.SyncConfig(
                syncer="auto",
                # Sync approximately every minute rather than on every checkpoint
                sync_on_checkpoint=False,
                sync_period=60,
            )
        )
    )
    tuner.fit()

In the snippet above, we disabled forceful syncing on trial checkpoints and adjusted the sync period to 60 seconds.
Setting the sync period to a lower value (in seconds) will sync from remote nodes more often.
This will lead to more robust trial recovery, but it will also lead to more synchronization overhead.

In this example, all experiment results can found on the head node at ``~/ray_results/experiment_name`` for further processing.

.. note::

    If you don't provide a :class:`~ray.tune.syncer.SyncConfig` at all, this is the default configuration.


.. tip::
    Please note that this approach is likely the least efficient one - you should always try to use
    shared or cloud storage if possible when training on a multi-node cluster.
    Using a network filesystem or cloud storage recommended when training a large number of distributed trials,
    since the default scenario with many worker nodes can introduce significant overhead.


Configuring Tune with a network filesystem (NFS)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If all Ray nodes have access to a network filesystem, e.g. AWS EFS or Google Cloud Filestore,
they can all write experiment outputs to this directory.

All we need to do is **set the shared network filesystem as the path to save results** and
**disable Ray Tune's default syncing behavior**.

.. code-block:: python
    :emphasize-lines: 7, 8, 9, 10

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

In this example, all experiment results can be found in the shared storage at ``/path/to/shared/storage/experiment_name`` for further processing.

.. _tune-cloud-checkpointing:

Configuring Tune with cloud storage (AWS S3, Google Cloud Storage)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If all nodes in a Ray cluster have access to cloud storage, e.g. AWS S3 or Google Cloud Storage (GCS),
then all experiment outputs can be saved in a shared cloud bucket.

We can configure cloud storage by telling Ray Tune to **upload to a remote** ``upload_dir``:

.. code-block:: python
    :emphasize-lines: 8, 9, 10, 11

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
if you want to implement custom logic for uploading/downloading from the cloud.
See :ref:`tune-cloud-syncing` and :ref:`tune-cloud-syncing-command-line-example`
for more details and examples of custom syncing.

In this example, all experiment results can be found in the shared storage at ``s3://bucket-name/sub-path/experiment_name`` ``/path/to/shared/storage/experiment_name`` for further processing.

.. note::

    The head node will not have access to all experiment results locally. If you want to process
    e.g. the best checkpoint further, you will first have to fetch it from the cloud storage.

    Experiment restoration should also be done using the experiment directory at the cloud storage
    URI, rather than the local experiment directory on the head node. See :ref:`here for an example <tune-syncing-restore-from-uri>`.


Examples
--------

Let's show some examples of configuring storage location and synchronization options.
We'll also show how to resume the experiment for each of the examples, in the case that your experiment gets interrupted.
See :ref:`tune-stopping-guide` for more information on resuming experiments.

In each example, we'll give a practical explanation of how *trial checkpoints* are saved
across the cluster and the external storage location (if one is provided).
See :ref:`tune-persisted-experiment-data` for an overview of other experiment data that Tune needs to persist.

Example: Running Tune with cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's assume that you're running this example script from your Ray cluster's head node.

In the example below, ``my_trainable`` is a Tune :ref:`trainable <trainable-docs>`
that implements saving and loading checkpoints.

.. code-block:: python

    import ray
    from ray import air, tune
    from your_module import my_trainable

    # Look for the existing cluster and connect to it
    ray.init()

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
                # (with the highest AUC scores, a metric reported by the trainable)
                checkpoint_score_attribute="max-auc",
                checkpoint_score_order="max",
                num_to_keep=5,
            ),
        ),
    )
    # This starts the run!
    results = tuner.fit()

In this example, here's how trial checkpoints will be saved:

- On head node where we are running from:
    - ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)
- On worker nodes:
    - ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)
- S3:
    - ``s3://my-checkpoints-bucket/path/my-tune-exp/<trial_name>/checkpoint_<step>`` (all trials)

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

.. _tune-default-syncing-example:

Example: Running Tune without external persistent storage (default scenario)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now, let's take a look at an example using default syncing behavior described above.
Again, we're running this example script from the Ray cluster's head node.

.. code-block:: python

    import ray
    from ray import tune
    from your_module import my_trainable

    # Look for the existing cluster and connect to it
    ray.init()

    # This starts the run!
    tuner = tune.Tuner(
        my_trainable,
        run_config=air.RunConfig(
            name="my-tune-exp",
            local_dir="/tmp/mypath",
            # Use the default syncing behavior
            # You don't have to pass an empty sync config - but we
            # do it here for clarity and comparison
            sync_config=tune.SyncConfig(),
            checkpoint_config=air.CheckpointConfig(
                checkpoint_score_attribute="max-auc",
                checkpoint_score_order="max",
                num_to_keep=5,
            ),
        )
    )

In this example, here's how trial checkpoints will be saved:

- On head node where we are running from:
    - ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (**all trials**, since they have been synced to the head node)
- On worker nodes:
    - ``/tmp/mypath/my-tune-exp/<trial_name>/checkpoint_<step>`` (but only for trials running on this node)

This experiment can be resumed from the head node:

.. code-block:: python

    from ray import tune
    tuner = tune.Tuner.restore(
        "/tmp/mypath/my-tune-exp",
        resume_errored=True
    )
    tuner.fit()

.. TODO: Move this appendix to a new tune-checkpoints user guide.

.. _tune-persisted-experiment-data:

Appendix: Types of Tune Experiment Data
---------------------------------------

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

Trial Results
~~~~~~~~~~~~~

Metrics reported by trials get saved and logged to their respective trial directories.
This is the data stored in csv/json format that can be inspected by Tensorboard and
used for post-experiment analysis.

Trial Checkpoints
~~~~~~~~~~~~~~~~~

Trial-level checkpoints capture the per-trial state. They are saved by the :ref:`trainable <tune_60_seconds_trainables>` itself.
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
