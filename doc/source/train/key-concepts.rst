.. _train-key-concepts:

Key Concepts
============

Trainers
--------
``Trainers`` are the centerpiece of Ray Train and are responsible for executing a (distributed) training run.
Trainers are configured with two main configuration objects:

* :class:`RunConfig <ray.air.config.RunConfig>`
* :class:`ScalingConfig <ray.air.config.ScalingConfig>`

The output of a Ray training run is a :ref:`result object <train-key-concepts-results>` that contains
metrics from the training run and the latest saved :ref:`model checkpoint <air-checkpoint-ref>`.

Each Trainer generates a framework-specific checkpoint, which is used with a framework-specific predictor.

.. list-table::

    * - **Trainer Class**
      - **Checkpoint Class**
      - **Predictor Class**
    * - :class:`TorchTrainer <ray.train.torch.TorchTrainer>`
      - :class:`TorchCheckpoint <ray.train.torch.TorchCheckpoint>`
      - :class:`TorchPredictor <ray.train.torch.TorchPredictor>`
    * - :class:`TensorflowTrainer <ray.train.tensorflow.TensorflowTrainer>`
      - :class:`TensorflowCheckpoint <ray.train.tensorflow.TensorflowCheckpoint>`
      - :class:`TensorflowPredictor <ray.train.tensorflow.TensorflowPredictor>`
    * - :class:`HorovodTrainer <ray.train.horovod.HorovodTrainer>`
      - Torch/Tensorflow Checkpoint
      - Torch/Tensorflow Predictor
    * - :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`
      - :class:`XGBoostCheckpoint <ray.train.xgboost.XGBoostCheckpoint>`
      - :class:`XGBoostPredictor <ray.train.xgboost.XGBoostPredictor>`
    * - :class:`LightGBMTrainer <ray.train.lightgbm.LightGBMTrainer>`
      - :class:`LightGBMCheckpoint <ray.train.lightgbm.LightGBMCheckpoint>`
      - :class:`LightGBMPredictor <ray.train.lightgbm.LightGBMPredictor>`
    * - :class:`SklearnTrainer <ray.train.sklearn.SklearnTrainer>`
      - :class:`SklearnCheckpoint <ray.train.sklearn.SklearnCheckpoint>`
      - :class:`SklearnPredictor <ray.train.sklearn.SklearnPredictor>`
    * - :class:`HuggingFaceTrainer <ray.train.huggingface.HuggingFaceTrainer>`
      - :class:`HuggingFaceCheckpoint <ray.train.huggingface.HuggingFaceCheckpoint>`
      - :class:`HuggingFacePredictor <ray.train.huggingface.HuggingFacePredictor>`
    * - :class:`RLTrainer <ray.train.rl.RLTrainer>`
      - :class:`RLCheckpoint <ray.train.rl.RLCheckpoint>`
      - :class:`RLPredictor <ray.train.rl.RLPredictor>`

Trainers can also handle :ref:`datasets <air-ingest>` and :ref:`preprocessors <air-preprocessors>` for
scalable data ingest and preprocessing.


.. tabbed::  Deep learning trainers

    Deep learning trainers utilize deep learning frameworks such as PyTorch, Tensorflow, or Horovod
    for training.

    For these trainers, you usually define your own training function. Please see the
    :ref:`Session <train-key-concepts-session>` section for details on how to interact with
    Ray Train in these functions.

    - :ref:`Deep learning user guide <train-dl-guide>`
    - :ref:`Quick overview of deep-learning trainers in the Ray AIR documentation <air-trainers-dl>`

    .. dropdown:: Example: :class:`TorchTrainer <ray.train.torch.TorchTrainer>`

        .. literalinclude:: /ray-air/doc_code/torch_trainer.py
            :language: python

.. tabbed::  Tree-based trainers

    Tree-based trainers utilize gradient-based decision trees for training. The most popular libraries
    for this are XGBoost and LightGBM.

    For these trainers, you just pass a dataset and parameters. The training loop is configured
    automatically.

    - :ref:`XGBoost/LightGBM user guide <train-gbdt-guide>`
    - :ref:`Quick overview of tree-based trainers in the Ray AIR documentation <air-trainers-tree>`

    .. dropdown:: Example: :class:`XGBoostTrainer <ray.train.xgboost.XGBoostTrainer>`

        .. literalinclude:: /ray-air/doc_code/xgboost_trainer.py
            :language: python


.. tabbed::  Other trainers

    Some trainers don't fit into the other two categories, such as the
    :class:`Huggingface trainer <ray.train.huggingface.HuggingfaceTrainer>` for NLP,
    the :class:`RL trainer <ray.train.rl.RLTrainer>` for reinforcement learning, and
    the :class:`SKlearn trainer <ray.train.sklearn.SKlearnTrainer>` for (non-distributed) training of
    SKlearn models.

    - :ref:`Quick overview of other trainers in the Ray AIR documentation <air-trainers-other>`

    .. dropdown:: Example: :class:`HuggingfaceTrainer <ray.train.huggingface.HuggingfaceTrainer>`

        .. literalinclude:: /ray-air/doc_code/hf_trainer.py
            :language: python
            :start-after: __hf_trainer_start__
            :end-before: __hf_trainer_end__

.. _train-key-concepts-config:

Configuration
-------------

Trainers is configured with configuration objects. There are two main configuration classes,
the :class:`ScalingConfig <ray.air.config.ScalingConfig>` and the :class:`RunConfig <ray.air.config.RunConfig>`.
The latter contains subconfigurations, such as the :class:`FailureConfig <ray.air.config.FailureConfig>`,
:class:`SyncConfig <ray.tune.syncer.SyncConfig>` and :class:`CheckpointConfig <ray.air.config.CheckpointConfig>`.

Scaling configuration (``ScalingConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scaling configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the scaling configuration are :ref:`tunable <air-tuner-search-space>`.

:class:`ScalingConfig API reference <ray.air.config.ScalingConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __scaling_config_start__
    :end-before: __scaling_config_end__


Run configuration (``RunConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The run configuration specifies distributed training properties like the number of workers or the
resources per worker.

The properties of the run configuration are :ref:`not tunable <air-tuner-search-space>`.

:class:`RunConfig API reference <ray.air.config.RunConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __run_config_start__
    :end-before: __run_config_end__

Failure configuration (``FailureConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The failure configuration specifies how training failures should be dealt with.

As part of the RunConfig, the properties of the failure configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`FailureConfig API reference <ray.air.config.FailureConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __failure_config_start__
    :end-before: __failure_config_end__

Sync configuration (``SyncConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The sync configuration specifies how to synchronize checkpoints between the
Ray cluster and remote storage.

As part of the RunConfig, the properties of the sync configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`SyncConfig API reference <ray.tune.syncer.SyncConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __sync_config_start__
    :end-before: __sync_config_end__


Checkpoint configuration (``CheckpointConfig``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The checkpoint configuration specifies how often to checkpoint training state
and how many checkpoints to keep.

As part of the RunConfig, the properties of the checkpoint configuration
are :ref:`not tunable <air-tuner-search-space>`.

:class:`CheckpointConfig API reference <ray.air.config.CheckpointConfig>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __checkpoint_config_start__
    :end-before: __checkpoint_config_end__


.. _train-key-concepts-session:

Session (DL Trainers)
---------------------

The session object is used to interact with the Ray Train training run from within your custom functions.

For instance, TorchTrainers and TensorflowTrainers accept
a ``train_loop_per_worker`` argument.
In this train loop, you can use :func:`session.report() <ray.air.session.report>` to report
training results or checkpoints to Ray Train.



.. tabbed::  Reporting results

    To report results, use :func:`session.report() <ray.air.session.report>` as in this example:

    .. literalinclude:: doc_code/key_concepts.py
        :language: python
        :start-after: __session_report_start__
        :end-before: __session_report_end__

.. tabbed::  Load and save checkpoints

    To load checkpoints, use :func:`session.get_checkpoint() <ray.air.session.get_checkpoint>`.

    To save checkpoints, pass them to :func:`session.report() <ray.air.session.report>`.

    .. literalinclude:: doc_code/key_concepts.py
        :language: python
        :start-after: __session_checkpoint_start__
        :end-before: __session_checkpoint_end__

.. tabbed::  Get dataset shard and worker info

    To get a sharded dataset in a data-parallel trainer, use :func:`session.get_dataset_shard() <ray.air.session.get_dataset_shard>`.

    To get information about the local/global rank and world sizes, see this example:

    .. literalinclude:: doc_code/key_concepts.py
        :language: python
        :start-after: __session_data_info_start__
        :end-before: __session_data_info_end__

.. _train-key-concepts-results:

Results
-------
Calling ``Trainer.fit()`` returns a :class:`Result <ray.air.result.Result>` object.

The result object contains information about the run, such as the reported metrics and the saved
checkpoints.

:class:`Result API reference <ray.air.result.Result>`

.. literalinclude:: doc_code/key_concepts.py
    :language: python
    :start-after: __results_start__
    :end-before: __results_end__

.. _train-key-concepts-predictors:

Predictors
----------

Predictors are the counterpart to Trainers. A Trainer trains a model on a dataset, and a predictor
uses the resulting model and performs inference on it.

Each Trainer has a respective Predictor implementation that is compatible with its generated checkpoints.

See :ref:`the Predictors user guide <air-predictors>` for more information and examples.

.. dropdown:: Example: :class:`XGBoostPredictor <ray.train.xgboost.XGBoostPredictor>`

    .. literalinclude:: /train/doc_code/xgboost_train_predict.py
        :language: python
        :start-after: __train_predict_start__
        :end-before: __train_predict_end__


BatchPredictor
~~~~~~~~~~~~~~

The BatchPredictor is used to scale up prediction over a Ray cluster. It takes
a Ray Dataset as input.

.. dropdown:: Example: Batch prediction with :class:`XGBoostPredictor <ray.train.xgboost.XGBoostPredictor>`

    .. literalinclude:: /train/doc_code/xgboost_train_predict.py
        :language: python
        :start-after: __batch_predict_start__
        :end-before: __batch_predict_end__