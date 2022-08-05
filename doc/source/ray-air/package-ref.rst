.. _air-api-ref:

API Reference
=============

.. contents::
    :local:

Components
----------

.. _air-preprocessor-ref:

Preprocessor
~~~~~~~~~~~~

.. autoclass:: ray.data.preprocessor.Preprocessor
    :members:

Generic Preprocessors
#####################

.. autoclass:: ray.data.preprocessors.BatchMapper
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.Chain
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.SimpleImputer
    :show-inheritance:

.. automethod:: ray.data.Dataset.train_test_split
    :noindex:

Category Preprocessors
######################

.. autoclass:: ray.data.preprocessors.Categorizer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.LabelEncoder
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.MultiHotEncoder
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.OneHotEncoder
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.OrdinalEncoder
    :show-inheritance:

Number Preprocessors
####################

.. autoclass:: ray.data.preprocessors.MaxAbsScaler
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.MinMaxScaler
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.Normalizer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.PowerTransformer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.RobustScaler
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.StandardScaler
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.Concatenator
    :show-inheritance:

Text Preprocessors
##################

.. autoclass:: ray.data.preprocessors.CountVectorizer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.FeatureHasher
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.HashingVectorizer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.Tokenizer
    :show-inheritance:

.. _air-abstract-trainer-ref:

Trainer
~~~~~~~

.. autoclass:: ray.train.trainer.BaseTrainer
    :members:

Abstract Classes
################

.. autoclass:: ray.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:

.. autoclass:: ray.train.gbdt_trainer.GBDTTrainer
    :members:
    :show-inheritance:

.. _air-results-ref:

Training Result
###############

.. automodule:: ray.air.result
    :members:

Training Session
################

.. automodule:: ray.air.session
    :members:

Trainer Configs
###############

.. automodule:: ray.air.config
    :members:

Checkpoint
~~~~~~~~~~

.. _air-checkpoint-ref:

.. automodule:: ray.air.checkpoint
    :members:

Predictor
~~~~~~~~~

.. autoclass:: ray.train.predictor.Predictor
    :members:

Data Types
##########

.. autoclass:: ray.train.predictor.DataBatchType

Batch Predictor
###############

.. autoclass:: ray.train.batch_predictor.BatchPredictor
    :members:

.. _air-tuner-ref:

Tuner
~~~~~

.. autoclass:: ray.tune.tuner.Tuner
    :members:

TuneConfig
##########

.. automodule:: ray.tune.tune_config
    :members:

Tuner Results
#############

.. automodule:: ray.tune.result_grid
    :members:

.. _air-serve-integration:

Serving
~~~~~~~

.. autoclass:: ray.serve.air_integrations.PredictorDeployment

.. autoclass:: ray.serve.air_integrations.PredictorWrapper

.. _air-trainer-ref:

Trainer and Predictor Integrations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

XGBoost
#######

.. automodule:: ray.train.xgboost
    :members:
    :show-inheritance:

LightGBM
########

.. automodule:: ray.train.lightgbm
    :members:
    :show-inheritance:

TensorFlow
##########

.. automodule:: ray.train.tensorflow
    :members:
    :show-inheritance:

PyTorch
#######

.. automodule:: ray.train.torch
    :members:
    :show-inheritance:

Horovod
#######

.. automodule:: ray.train.horovod
    :members:
    :show-inheritance:

HuggingFace
###########

.. automodule:: ray.train.huggingface
    :members:
    :show-inheritance:

Scikit-Learn
############

.. automodule:: ray.train.sklearn
    :members:
    :show-inheritance:

.. _air-builtin-callbacks:

Monitoring Integrations
~~~~~~~~~~~~~~~~~~~~~~~

Comet
#####

.. autoclass:: ray.air.callbacks.comet.CometLoggerCallback

Keras
#####

.. autoclass:: ray.air.callbacks.keras.Callback
    :members:

MLflow
######

.. autoclass:: ray.air.callbacks.mlflow.MLflowLoggerCallback

Weights and Biases
##################

.. autoclass:: ray.air.callbacks.wandb.WandbLoggerCallback

.. _air-session-ref:
