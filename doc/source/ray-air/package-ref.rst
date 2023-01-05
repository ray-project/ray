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

K-Bins Discretizers
###################

.. autoclass:: ray.data.preprocessors.CustomKBinsDiscretizer
    :show-inheritance:

.. autoclass:: ray.data.preprocessors.UniformKBinsDiscretizer
    :show-inheritance:

Image Preprocessors
###################

.. autoclass:: ray.data.preprocessors.TorchVisionPreprocessor
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

.. autoclass:: ray.air.util.check_ingest.DummyTrainer
    :members:
    :show-inheritance:

    .. automethod:: __init__

.. _air-results-ref:

Dataset Iteration
#################

.. autoclass:: ray.air.DatasetIterator
    :members:

.. autofunction:: ray.air.util.check_ingest.make_local_dataset_iterator

Training Result
###############

.. automodule:: ray.air.result
    :members:

.. _air-session-ref:

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
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.xgboost
    :members:
    :exclude-members: XGBoostTrainer
    :show-inheritance:
    :noindex:

LightGBM
########

.. autoclass:: ray.train.lightgbm.LightGBMTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.lightgbm
    :members:
    :exclude-members: LightGBMTrainer
    :show-inheritance:
    :noindex:

TensorFlow
##########

.. autoclass:: ray.train.tensorflow.TensorflowTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.tensorflow
    :members:
    :exclude-members: TensorflowTrainer
    :show-inheritance:
    :noindex:

.. _air-pytorch-ref:

PyTorch
#######

.. autoclass:: ray.train.torch.TorchTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.torch
    :members:
    :exclude-members: TorchTrainer
    :show-inheritance:
    :noindex:

Horovod
#######

.. autoclass:: ray.train.horovod.HorovodTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.horovod
    :members:
    :exclude-members: HorovodTrainer
    :show-inheritance:
    :noindex:

HuggingFace
###########

.. autoclass:: ray.train.huggingface.HuggingFaceTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.huggingface
    :members:
    :exclude-members: HuggingFaceTrainer
    :show-inheritance:
    :noindex:

Scikit-Learn
############

.. autoclass:: ray.train.sklearn.SklearnTrainer
    :members:
    :show-inheritance:
    :noindex:

    .. automethod:: __init__
        :noindex:


.. automodule:: ray.train.sklearn
    :members:
    :exclude-members: SklearnTrainer
    :show-inheritance:
    :noindex:


Reinforcement Learning (RLlib)
##############################

.. automodule:: ray.train.rl
    :members:
    :show-inheritance:
    :noindex:

.. _air-builtin-callbacks:

Monitoring Integrations
~~~~~~~~~~~~~~~~~~~~~~~

Comet
#####

.. autoclass:: ray.air.integrations.comet.CometLoggerCallback

Keras
#####

.. autoclass:: ray.air.integrations.keras.Callback
    :members:

MLflow
######

.. autoclass:: ray.air.integrations.mlflow.MLflowLoggerCallback

Weights and Biases
##################

.. autoclass:: ray.air.integrations.wandb.WandbLoggerCallback
