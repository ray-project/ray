:orphan:

.. _train-deprecated-api:

Ray Train V1 API
================

.. currentmodule:: ray

.. important::

    Ray Train V2 is an overhaul of Ray Train's implementation and select APIs, which can be enabled by setting the environment variable ``RAY_TRAIN_V2_ENABLED=1`` starting in Ray 2.43.

    This page contains the deprecated V1 API references. See :ref:`train-api` for the new V2 API references and the `Ray Train V2 Migration Guide <https://github.com/ray-project/ray/issues/49454>`_.


PyTorch Ecosystem
-----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.torch_trainer.TorchTrainer

.. autosummary::
    :nosignatures:

    ~train.torch.TorchConfig
    ~train.torch.xla.TorchXLAConfig

PyTorch
~~~~~~~

.. autosummary::
    :nosignatures:

    ~train.torch.get_device
    ~train.torch.get_devices
    ~train.torch.prepare_model
    ~train.torch.prepare_data_loader
    ~train.torch.enable_reproducibility

PyTorch Lightning
~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:

    ~train.lightning.prepare_trainer
    ~train.lightning.RayLightningEnvironment
    ~train.lightning.RayDDPStrategy
    ~train.lightning.RayFSDPStrategy
    ~train.lightning.RayDeepSpeedStrategy
    ~train.lightning.RayTrainReportCallback

Hugging Face Transformers
~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:

    ~train.huggingface.transformers.prepare_trainer
    ~train.huggingface.transformers.RayTrainReportCallback


More Frameworks
---------------

TensorFlow/Keras
~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.tensorflow.tensorflow_trainer.TensorflowTrainer

.. autosummary::
    :nosignatures:

    ~train.tensorflow.TensorflowConfig
    ~train.tensorflow.prepare_dataset_shard
    ~train.tensorflow.keras.ReportCheckpointCallback

Horovod
~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.horovod.HorovodTrainer
    ~train.horovod.HorovodConfig


XGBoost
~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.xgboost.xgboost_trainer.XGBoostTrainer

.. autosummary::
    :nosignatures:

    ~train.xgboost.RayTrainReportCallback


LightGBM
~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightgbm.lightgbm_trainer.LightGBMTrainer

.. autosummary::
    :nosignatures:

    ~train.lightgbm.RayTrainReportCallback


Ray Train Configuration
-----------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~air.config.ScalingConfig
    ~air.config.RunConfig
    ~air.config.FailureConfig

.. autosummary::
    :nosignatures:

    ~train.CheckpointConfig
    ~train.DataConfig

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.SyncConfig


Ray Train Utilities
-------------------

**Classes**

.. autosummary::
    :nosignatures:

    ~train.Checkpoint

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.context.TrainContext

**Functions**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train._internal.session.get_checkpoint
    ~train.context.get_context
    ~train._internal.session.get_dataset_shard
    ~train._internal.session.report


Ray Train Output
----------------

.. autosummary::
    :nosignatures:

    ~train.Result

Ray Train Errors
----------------

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.error.SessionMisuseError
    ~train.base_trainer.TrainingFailedError


Ray Train Developer APIs
------------------------

Trainer Base Classes
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.trainer.BaseTrainer
    ~train.data_parallel_trainer.DataParallelTrainer


Train Backend Base Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst

    ~train.backend.Backend
    ~train.backend.BackendConfig
