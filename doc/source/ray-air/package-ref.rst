.. _air-api-ref:

Ray AIR API
===========

.. contents::
    :local:

Components
----------

.. _air-preprocessor-ref:

Preprocessors
~~~~~~~~~~~~~

.. autoclass:: ray.air.preprocessor.Preprocessor
    :members:

.. automodule:: ray.air.preprocessors
    :members:
    :show-inheritance:

.. autofunction:: ray.air.train_test_split


.. _air-trainer-ref:

Trainer
~~~~~~~

.. autoclass:: ray.train.trainer.BaseTrainer
    :members:

.. automodule:: ray.train.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.train.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.train.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.train.torch
    :members:
    :show-inheritance:

.. automodule:: ray.train.horovod
    :members:
    :show-inheritance:

.. automodule:: ray.train.huggingface
    :members:
    :show-inheritance:

.. automodule:: ray.train.sklearn
    :members:
    :show-inheritance:

.. autoclass:: ray.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:

.. autoclass:: ray.train.gbdt_trainer.GBDTTrainer
    :members:
    :show-inheritance:



.. _air-tuner-ref:

Tuner
~~~~~

.. autoclass:: ray.tune.tuner.Tuner
    :members:

.. automodule:: ray.tune.result_grid
    :members:

Predictors
~~~~~~~~~~

.. autoclass:: ray.air.predictor.Predictor
    :members:

.. autoclass:: ray.air.predictor.DataBatchType

.. autoclass:: ray.air.batch_predictor.BatchPredictor
    :members:

.. automodule:: ray.air.predictors.integrations.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.air.predictors.integrations.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.air.predictors.integrations.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.air.predictors.integrations.torch
    :members:
    :show-inheritance:

.. automodule:: ray.air.predictors.integrations.sklearn
    :members:
    :show-inheritance:

.. automodule:: ray.air.predictors.integrations.huggingface
    :members:
    :show-inheritance:

.. _air-serve-integration:

Serving
~~~~~~~

.. autoclass:: ray.serve.model_wrappers.ModelWrapperDeployment

.. autoclass:: ray.serve.model_wrappers.ModelWrapper

.. _air-results-ref:

Outputs
~~~~~~~

.. _air-checkpoint-ref:

Checkpoint
##########

.. automodule:: ray.air.checkpoint
    :members:

Result
######

.. automodule:: ray.air.result
    :members:


Configs
~~~~~~~

.. automodule:: ray.air.config
    :members:

