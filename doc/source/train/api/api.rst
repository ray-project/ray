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

Ray Train Integrations
----------------------

Ray Train Built-in Trainers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~xgboost.XGBoostTrainer
    ~lightgbm.LightGBMTrainer
    ~tensorflow.TensorflowTrainer
    ~torch.TorchTrainer
    ~horovod.HorovodTrainer
    ~huggingface.HuggingFaceTrainer
    ~sklearn.SklearnTrainer
    ~mosaic.MosaicTrainer
    ~rl.RLTrainer

.. _train-framework-specific-ckpts:

Ray Train Framework-specific Checkpoints
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~tensorflow.TensorflowCheckpoint
    ~torch.TorchCheckpoint
    ~xgboost.XGBoostCheckpoint
    ~lightgbm.LightGBMCheckpoint
    ~huggingface.HuggingFaceCheckpoint
    ~sklearn.SklearnCheckpoint
    ~rl.RLCheckpoint
