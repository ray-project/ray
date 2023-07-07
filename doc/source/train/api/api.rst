.. _train-api:
.. _air-trainer-ref:

Ray Train API
=============

This page covers framework specific integrations with Ray Train and Ray Train Developer APIs.

For core Ray AIR APIs, take a look at the :ref:`AIR package reference <air-api-ref>`.

Ray Train Base Classes (Developer APIs)
---------------------------------------

.. currentmodule:: ray

.. _train-base-trainer:

Trainer Base Classes
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.trainer.BaseTrainer
    ~train.data_parallel_trainer.DataParallelTrainer
    ~train.data_config.DataConfig
    ~train.gbdt_trainer.GBDTTrainer

``BaseTrainer`` API
*******************

.. autosummary::
    :toctree: doc/

    ~train.trainer.BaseTrainer.fit
    ~train.trainer.BaseTrainer.setup
    ~train.trainer.BaseTrainer.preprocess_datasets
    ~train.trainer.BaseTrainer.training_loop
    ~train.trainer.BaseTrainer.as_trainable


Train Backend Base Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _train-backend:
.. _train-backend-config:

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_without_autosummary.rst

    ~train.backend.Backend
    ~train.backend.BackendConfig


.. _train-integration-api:
.. _train-framework-specific-ckpts:

Ray Train Integrations
----------------------

.. _train-pytorch-integration:

PyTorch
~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.torch.TorchTrainer
    ~train.torch.TorchConfig
    ~train.torch.TorchCheckpoint


PyTorch Training Loop Utilities
********************************

.. autosummary::
    :toctree: doc/

    ~train.torch.prepare_model
    ~train.torch.prepare_optimizer
    ~train.torch.prepare_data_loader
    ~train.torch.get_device
    ~train.torch.accelerate
    ~train.torch.backward
    ~train.torch.enable_reproducibility

.. _train-lightning-integration:

PyTorch Lightning
~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.lightning.LightningTrainer
    ~train.lightning.LightningConfigBuilder
    ~train.lightning.LightningCheckpoint
    ~train.lightning.LightningPredictor

Tensorflow/Keras
~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.tensorflow.TensorflowTrainer
    ~train.tensorflow.TensorflowConfig
    ~train.tensorflow.TensorflowCheckpoint


Tensorflow/Keras Training Loop Utilities
****************************************

.. autosummary::
    :toctree: doc/

    ~train.tensorflow.prepare_dataset_shard

.. autosummary::

    ~air.integrations.keras.ReportCheckpointCallback


Horovod
~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.horovod.HorovodTrainer
    ~train.horovod.HorovodConfig


XGBoost
~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.xgboost.XGBoostTrainer
    ~train.xgboost.XGBoostCheckpoint


LightGBM
~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.lightgbm.LightGBMTrainer
    ~train.lightgbm.LightGBMCheckpoint


Hugging Face
~~~~~~~~~~~~

Transformers
************

.. autosummary::
    :toctree: doc/

    ~train.huggingface.TransformersTrainer
    ~train.huggingface.TransformersCheckpoint

Accelerate
**********

.. autosummary::
    :toctree: doc/

    ~train.huggingface.AccelerateTrainer

Scikit-Learn
~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.sklearn.SklearnTrainer
    ~train.sklearn.SklearnCheckpoint


Mosaic
~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.mosaic.MosaicTrainer


Reinforcement Learning (RLlib)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~train.rl.RLTrainer
    ~train.rl.RLCheckpoint


.. _trainer-restore:

Ray Train Experiment Restoration
--------------------------------

.. autosummary::
    :toctree: doc/

    train.trainer.BaseTrainer.restore

.. note::

    All trainer classes have a `restore` method that takes in a path
    pointing to the directory of the experiment to be restored.
    `restore` also exposes a subset of construtor arguments that can be re-specified.
    See :ref:`train-framework-specific-restore`
    below for details on `restore` arguments for different AIR trainer integrations.

.. _train-framework-specific-restore:

Restoration API for Built-in Trainers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    train.data_parallel_trainer.DataParallelTrainer.restore

.. autosummary::

    train.huggingface.TransformersTrainer.restore

.. note::

    `TorchTrainer.restore`, `TensorflowTrainer.restore`, and `HorovodTrainer.restore`
    can take in the same parameters as their parent class's
    :meth:`DataParallelTrainer.restore <ray.train.data_parallel_trainer.DataParallelTrainer.restore>`.

    Unless otherwise specified, other trainers will accept the same parameters as
    :meth:`BaseTrainer.restore <ray.train.trainer.BaseTrainer.restore>`.

.. seealso::

    See :ref:`train-restore-guide` for more details on when and how trainer restore should be used.
