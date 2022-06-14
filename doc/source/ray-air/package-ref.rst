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

.. autoclass:: ray.data.preprocessor.Preprocessor
    :members:

.. automodule:: ray.data.preprocessors
    :members:
    :show-inheritance:

.. autofunction:: ray.air.train_test_split


.. _air-trainer-ref:

Trainer
~~~~~~~

.. autoclass:: ray.train.trainer.BaseTrainer
    :members:

.. automodule:: ray.train.xgboost.xgboost_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.lightgbm.lightgbm_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.tensorflow.tensorflow_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.torch.torch_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.horovod.horovod_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.huggingface.huggingface_trainer
    :members:
    :show-inheritance:

.. automodule:: ray.train.sklearn.sklearn_trainer
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

.. autoclass:: ray.train.predictor.Predictor
    :members:

.. autoclass:: ray.train.predictor.DataBatchType

.. autoclass:: ray.train.batch_predictor.BatchPredictor
    :members:

.. automodule:: ray.train.xgboost.xgboost_predictor
    :members:
    :show-inheritance:

.. automodule:: ray.train.lightgbm.lightgbm_predictor
    :members:
    :show-inheritance:

.. automodule:: ray.train.tensorflow.tensorflow_predictor
    :members:
    :show-inheritance:

.. automodule:: ray.train.torch.torch_predictor
    :members:
    :show-inheritance:

.. automodule:: ray.train.sklearn.sklearn_predictor
    :members:
    :show-inheritance:

.. automodule:: ray.train.huggingface.huggingface_predictor
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

