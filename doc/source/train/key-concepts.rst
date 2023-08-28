.. _train-key-concepts:

Key Concepts of Ray Train
=========================

There are three main concepts in the Ray Train library.

1. ``Trainers`` execute distributed training.
2. ``Configuration`` objects are used to configure training.
3. ``Checkpoints`` are returned as the result of training.

.. https://docs.google.com/drawings/d/1FezcdrXJuxLZzo6Rjz1CHyJzseH8nPFZp6IUepdn3N4/edit

.. image:: images/train-specific.svg

Trainers
--------

Trainers are responsible for executing (distributed) training runs.
The output of a Trainer run is a :ref:`Result <train-key-concepts-results>` that contains
metrics from the training run and the latest saved :class:`~ray.train.Checkpoint`.
You can also configured trainers with :ref:`Datasets <data-ingest-torch>` and :ref:`Preprocessors <air-preprocessors>` for scalable data ingest and preprocessing.


Deep Learning, Tree-Based, and other Trainers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are two categories of built-in Trainers:

.. tab-set::

    .. tab-item:: Deep Learning Trainers

        Ray Train supports the following deep learning trainers:

        - :class:`TorchTrainer <ray.train.torch.TorchTrainer>`
        - :class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`
        - :class:`HorovodTrainer <ray.train.horovod.HorovodTrainer>`

        For these trainers, you usually define your own training function that loads the model
        and executes single-worker training steps. Refer to the following guides for more details:

        - :doc:`Distributed PyTorch </train/getting-started-pytorch>`
        - :doc:`Distributed TensorFlow </train/distributed-tensorflow-keras>`
        - :doc:`Horovod </train/horovod>`

    .. tab-item:: Tree-Based Trainers

        Tree-based trainers utilize gradient-based decision trees for training. The most popular libraries
        for this are XGBoost and LightGBM.

        - :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`
        - :class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`

        For these trainers, you just pass a dataset and parameters. The training loop is configured
        automatically.

        - :doc:`Distributed XGBoost/LightGBM </train/distributed-xgboost-lightgbm>`


.. _train-key-concepts-config:

Train Configuration
-------------------

Trainers are configured with configuration objects. There are two main configuration classes,
the :class:`ScalingConfig <ray.air.config.ScalingConfig>` and the :class:`RunConfig <ray.air.config.RunConfig>`.
The latter contains subconfigurations, such as the :class:`FailureConfig <ray.air.config.FailureConfig>`,
:class:`SyncConfig <ray.train.SyncConfig>` and :class:`CheckpointConfig <ray.air.config.CheckpointConfig>`.

.. _train-key-concepts-results:

Train Checkpoints
-----------------

Calling ``Trainer.fit()`` returns a :class:`Result <ray.train.Result>` object, which includes
information about the run such as the reported metrics and the saved checkpoints.

Checkpoints have the following purposes:

* They can be passed to a Trainer to resume training from the given model state.
* They can be used with Ray Data for scalable batch prediction.
* They can be deployed with Ray Serve.
