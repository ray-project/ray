.. _air-api-ref:

AIR API
=======

.. contents::
    :local:

Components
----------

Preprocessors
~~~~~~~~~~~~~

.. autoclass:: ray.ml.preprocessor.Preprocessor
    :members:

.. automodule:: ray.ml.preprocessors
    :members:
    :show-inheritance:

.. autofunction:: ray.ml.train_test_split


.. _air-trainer-ref:

Trainer
~~~~~~~

.. autoclass:: ray.ml.trainer.Trainer
    :members:

.. automodule:: ray.ml.train.integrations.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.torch
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.huggingface
    :members:
    :show-inheritance:

.. automodule:: ray.ml.train.integrations.sklearn
    :members:
    :show-inheritance:

.. autoclass:: ray.ml.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:

.. autoclass:: ray.ml.train.gbdt_trainer.GBDTTrainer
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

.. autoclass:: ray.ml.predictor.Predictor
    :members:

.. autoclass:: ray.ml.batch_predictor.BatchPredictor
    :members:

.. automodule:: ray.ml.predictors.integrations.xgboost
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.lightgbm
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.tensorflow
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.torch
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.sklearn
    :members:
    :show-inheritance:

.. automodule:: ray.ml.predictors.integrations.huggingface
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

.. automodule:: ray.ml.checkpoint
    :members:


.. automodule:: ray.ml.result
    :members:


Configs
~~~~~~~

.. automodule:: ray.ml.config
    :members:

