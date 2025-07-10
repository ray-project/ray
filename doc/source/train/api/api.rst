.. _train-api:

Ray Train API
=============

.. currentmodule:: ray


.. important::

    These API references are for the revamped Ray Train V2 implementation that is available starting from Ray 2.43
    by enabling the environment variable ``RAY_TRAIN_V2_ENABLED=1``. These APIs assume that the environment variable has been enabled.

    See :ref:`train-deprecated-api` for the old API references and the `Ray Train V2 Migration Guide <https://github.com/ray-project/ray/issues/49454>`_.


PyTorch Ecosystem
-----------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.torch.TorchTrainer
    ~train.torch.TorchConfig
    ~train.torch.xla.TorchXLAConfig

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


XGBoost
~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.xgboost.XGBoostTrainer
    ~train.xgboost.RayTrainReportCallback


LightGBM
~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.lightgbm.LightGBMTrainer
    ~train.lightgbm.RayTrainReportCallback


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

.. _train-loop-api:

Ray Train Utilities
-------------------

**Classes**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.Checkpoint
    ~train.v2.api.context.TrainContext

**Functions**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.get_checkpoint
    ~train.get_context
    ~train.get_dataset_shard
    ~train.report

**Collective**

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.collective.barrier
    ~train.collective.broadcast_from_rank_zero

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

    ~train.v2.api.exceptions.TrainingFailedError

Ray Tune Integration Utilities
------------------------------

.. autosummary::
    :nosignatures:
    :toctree: doc/

    tune.integration.ray_train.TuneReportCallback


Ray Train Developer APIs
------------------------

Trainer Base Class
~~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.v2.api.data_parallel_trainer.DataParallelTrainer

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

Trainer Callbacks
~~~~~~~~~~~~~~~~~

.. autosummary::
    :nosignatures:
    :toctree: doc/

    ~train.UserCallback
