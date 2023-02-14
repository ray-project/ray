.. _train-api:
.. _air-trainer-ref:

Ray Train API
=============

This page covers framework specific integrations with Ray Train and Ray Train Developer APIs.

For core Ray AIR APIs, take a look at the :ref:`AIR package reference <air-api-ref>`.

Ray Train Base Classes (Developer APIs)
---------------------------------------

.. currentmodule:: ray.train

.. _train-base-trainer:

Trainer Base Classes
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/
    :template: autosummary/class_with_autosummary.rst

    ~trainer.BaseTrainer
    ~data_parallel_trainer.DataParallelTrainer
    ~gbdt_trainer.GBDTTrainer

``BaseTrainer`` Methods
************************

.. autosummary::
    :toctree: doc/

    ~trainer.BaseTrainer.fit
    ~trainer.BaseTrainer.setup
    ~trainer.BaseTrainer.preprocess_datasets
    ~trainer.BaseTrainer.training_loop
    ~trainer.BaseTrainer.as_trainable


Train Backend Base Classes
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. _train-backend:
.. _train-backend-config:

.. autosummary::
    :toctree: doc/

    backend.Backend
    backend.BackendConfig


.. _train-integration-api:
.. _train-framework-specific-ckpts:

Ray Train Integrations
----------------------

.. _train-pytorch-integration:

Pytorch
~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~torch.TorchTrainer
    ~torch.prepare_model
    ~torch.prepare_optimizer
    ~torch.prepare_data_loader
    ~torch.get_device
    ~torch.accelerate
    ~torch.backward
    ~torch.enable_reproducibility
    ~torch.TorchConfig

.. autosummary::

    ~torch.TorchCheckpoint


Tensorflow/Keras
~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tensorflow.TensorflowTrainer
    ~tensorflow.prepare_dataset_shard
    ~tensorflow.TensorflowConfig

.. autosummary::

    ~tensorflow.TensorflowCheckpoint
    ~air.integrations.keras.ReportCheckpointCallback


Horovod
~~~~~~~

.. autosummary::
    :toctree: doc/

    ~horovod.HorovodTrainer
    ~horovod.HorovodConfig


XGBoost
~~~~~~~

.. autosummary::
    :toctree: doc/

    ~xgboost.XGBoostTrainer


.. autosummary::

    ~xgboost.XGBoostCheckpoint


LightGBM
~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~lightgbm.LightGBMTrainer


.. autosummary::

    ~lightgbm.LightGBMCheckpoint


HuggingFace
~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~huggingface.HuggingFaceTrainer

.. autosummary::

    ~huggingface.HuggingFaceCheckpoint


Scikit-Learn
~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~sklearn.SklearnTrainer

.. autosummary::

    ~sklearn.SklearnCheckpoint


Mosaic
~~~~~~

.. autosummary::
    :toctree: doc/

    ~mosaic.MosaicTrainer


Reinforcement Learning (RLlib)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~rl.RLTrainer

.. autosummary::

    ~rl.RLCheckpoint
