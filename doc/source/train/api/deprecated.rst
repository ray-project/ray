:orphan:

.. _train-deprecated-api:

Ray Train API
=============

.. currentmodule:: ray

PyTorch Ecosystem
-----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.torch_trainer.TorchTrainer
    ~train.torch.TorchConfig
    ~train.torch.xla.TorchXLAConfig

PyTorch
~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.get_device
    ~train.torch.get_devices
    ~train.torch.prepare_model
    ~train.torch.prepare_data_loader
    ~train.torch.enable_reproducibility

PyTorch Lightning
~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

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
    :toctree: doc/

    ~train.huggingface.transformers.prepare_trainer
    ~train.huggingface.transformers.RayTrainReportCallback


More Frameworks
---------------

Tensorflow/Keras
~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.tensorflow.TensorflowTrainer
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
    ~train.xgboost.RayTrainReportCallback


LightGBM
~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightgbm.lightgbm_trainer.LightGBMTrainer
    ~train.lightgbm.RayTrainReportCallback


Ray Train Configuration
-----------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.CheckpointConfig
    ~train.DataConfig
    ~train.FailureConfig
    ~train.RunConfig
    ~train.ScalingConfig
    ~train.SyncConfig

Ray Train Utilities
-------------------

**Classes**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.Checkpoint
    ~train.context.TrainContext

**Functions**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.get_checkpoint
    ~train.get_context
    ~train.get_dataset_shard
    ~train.report


Ray Train Output
----------------

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.Result

Ray Train Errors
----------------

.. autosummary::
    :nosignatures:
    :template: autosummary/class_without_autosummary.rst
    :toctree: doc/

    ~train.error.SessionMisuseError
    ~train.base_trainer.TrainingFailedError


Ray Tune Integration
--------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    tune.integration.ray_train.TuneReportCallback


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
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ~train.backend.Backend
    ~train.backend.BackendConfig
