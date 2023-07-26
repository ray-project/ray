.. _train-persistent-storage:

Persistent storage
==================


Configuring checkpoint persistence
----------------------------------
Per default, all checkpoints are stored and retained at the ``storage_path`` location.

To save space, you can configure how many and which checkpoints are retained. For this,
Ray Train provides the :py:class:`~ray.air.config.CheckpointConfig` class.

As an example, to completely disable writing checkpoints to disk:

.. code-block:: python
    :emphasize-lines: 9,14

    from ray.air import session, RunConfig, CheckpointConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func():
        for epoch in range(3):
            checkpoint = Checkpoint.from_dict(dict(epoch=epoch))
            session.report({}, checkpoint=checkpoint)

    checkpoint_config = CheckpointConfig(num_to_keep=0)

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(checkpoint_config=checkpoint_config)
    )
    trainer.fit()


You may also config ``CheckpointConfig`` to keep the "N best" checkpoints persisted to disk. The following example shows how you could keep the 2 checkpoints with the lowest "loss" value:

.. code-block:: python

    from ray.air import session, Checkpoint, RunConfig, CheckpointConfig, ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func():
        # first checkpoint
        session.report(dict(loss=2), checkpoint=Checkpoint.from_dict(dict(loss=2)))
        # second checkpoint
        session.report(dict(loss=2), checkpoint=Checkpoint.from_dict(dict(loss=4)))
        # third checkpoint
        session.report(dict(loss=2), checkpoint=Checkpoint.from_dict(dict(loss=1)))
        # fourth checkpoint
        session.report(dict(loss=2), checkpoint=Checkpoint.from_dict(dict(loss=3)))

    # Keep the 2 checkpoints with the smallest "loss" value.
    checkpoint_config = CheckpointConfig(
        num_to_keep=2, checkpoint_score_attribute="loss", checkpoint_score_order="min"
    )

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2),
        run_config=RunConfig(checkpoint_config=checkpoint_config),
    )
    result = trainer.fit()
    print(result.best_checkpoints[0][0].get_internal_representation())
    # ('local_path', '/home/ubuntu/ray_results/TorchTrainer_2022-06-24_21-34-49/TorchTrainer_7988b_00000_0_2022-06-24_21-34-49/checkpoint_000000')
    print(result.best_checkpoints[1][0].get_internal_representation())
    # ('local_path', '/home/ubuntu/ray_results/TorchTrainer_2022-06-24_21-34-49/TorchTrainer_7988b_00000_0_2022-06-24_21-34-49/checkpoint_000002')


Synchronization configurations in Train (``tune.SyncConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``tune.SyncConfig`` specifies how data is synchronized to a remote storage path.


As part of the RunConfig, the properties of the failure configuration
are :ref:`not tunable <tune-search-space-tutorial>`.

.. note::

    This configuration is mostly relevant to running multiple Train runs with a
    Ray Tune. See :ref:`tune-storage-options` for a guide on using the ``SyncConfig``.

.. seealso::

    See the :class:`~ray.tune.syncer.SyncConfig` API reference.
