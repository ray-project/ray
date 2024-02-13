.. _train-api:

Ray Train API
=============

.. _train-integration-api:
.. _train-framework-specific-ckpts:

.. currentmodule:: ray

PyTorch Ecosystem
-----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.TorchTrainer
    ~train.torch.TorchConfig

.. _train-pytorch-integration:

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

.. _train-lightning-integration:

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

.. _train-transformers-integration:

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

    ~train.xgboost.XGBoostTrainer


LightGBM
~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightgbm.LightGBMTrainer


.. _ray-train-configs-api:

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

.. _train-loop-api:

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


Ray Train Developer APIs
------------------------

.. _train-base-trainer:

Trainer Base Classes
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.trainer.BaseTrainer
    ~train.data_parallel_trainer.DataParallelTrainer
    ~train.gbdt_trainer.GBDTTrainer


Train Backend Base Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _train-backend:
.. _train-backend-config:

.. autosummary::
    :nosignatures:
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ~train.backend.Backend
    ~train.backend.BackendConfig
