.. _air-api-ref:

Ray AIR API
===========

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

.. autoclass:: ray.data.preprocessors.Concatenator
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.SimpleImputer
    :show-inheritance:

.. automethod:: ray.data.Dataset.train_test_split
    :noindex:

Categorical Encoders
####################

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

Feature Scalers
###############

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

Text Encoders
#############

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

    .. automethod:: __init__

Abstract Classes
################

.. autoclass:: ray.train.data_parallel_trainer.DataParallelTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

.. autoclass:: ray.train.gbdt_trainer.GBDTTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

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

.. autoclass:: ray.train.xgboost.XGBoostTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.xgboost
    :members:
    :exclude-members: XGBoostTrainer
    :show-inheritance:

LightGBM
########

.. autoclass:: ray.train.lightgbm.LightGBMTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.lightgbm
    :members:
    :exclude-members: LightGBMTrainer
    :show-inheritance:

TensorFlow
##########

.. autoclass:: ray.train.tensorflow.TensorflowTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.tensorflow
    :members:
    :exclude-members: TensorflowTrainer
    :show-inheritance:

.. _air-pytorch-ref:

PyTorch
#######

.. autoclass:: ray.train.torch.TorchTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.torch
    :members:
    :exclude-members: TorchTrainer
    :show-inheritance:

Horovod
#######

.. autoclass:: ray.train.horovod.HorovodTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.horovod
    :members:
    :exclude-members: HorovodTrainer
    :show-inheritance:

HuggingFace
###########

.. autoclass:: ray.train.huggingface.HuggingFaceTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.huggingface
    :members:
    :exclude-members: HuggingFaceTrainer
    :show-inheritance:

Scikit-Learn
############

.. autoclass:: ray.train.sklearn.SklearnTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__


.. automodule:: ray.train.sklearn
    :members:
    :exclude-members: SklearnTrainer
    :show-inheritance:


Reinforcement Learning (RLlib)
##############################

.. automodule:: ray.train.rl
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
