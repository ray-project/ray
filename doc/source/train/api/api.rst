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

``BaseTrainer`` API
************************

.. autosummary::
    :toctree: doc/

    ~trainer.BaseTrainer.fit
    ~trainer.BaseTrainer.setup
    ~trainer.BaseTrainer.preprocess_datasets
    ~trainer.BaseTrainer.training_loop
    ~trainer.BaseTrainer.as_trainable


Trainer Restoration
*******************

.. autosummary::
    :toctree: doc/

    ~trainer.BaseTrainer.restore

.. note::

    All trainer classes have a `restore` method which exposes the construtor arguments
    that can be re-specified. `restore` always takes in a path pointing to the
    directory of the experiment to be restored. See :ref:`train-framework-specific-restore`
    for details on `restore` arguments for different AIR trainer integrations.


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


.. _train-framework-specific-restore:

Restoration API for Built-in Trainers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    data_parallel_trainer.DataParallelTrainer.restore
    gbdt_trainer.GBDTTrainer.restore
    huggingface.HuggingFaceTrainer.restore

.. note::

    `TorchTrainer`, `TensorflowTrainer`, and `HorovodTrainer` fall under `DataParallelTrainer`.

    `XGBoostTrainer` and `LightGBMTrainer` fall under `GBDTTrainer`.


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
