Predictor
=========

.. currentmodule:: ray.train

Predictor Interface
-------------------

Constructor Options
~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    predictor.Predictor

.. autosummary::
    :toctree: doc/

    predictor.Predictor.from_checkpoint
    predictor.Predictor.from_pandas_udf

Predictor Properties
~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    predictor.Predictor.get_preprocessor
    predictor.Predictor.set_preprocessor


Prediction API
~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    predictor.Predictor.predict


Supported Data Formats
~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    predictor.Predictor.preferred_batch_format
    ~predictor.DataBatchType

.. _air_framework_predictors:

Built-in Predictors for Library Integrations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~xgboost.XGBoostPredictor
    ~lightgbm.LightGBMPredictor
    ~tensorflow.TensorflowPredictor
    ~torch.TorchPredictor
    ~huggingface.TransformersPredictor
    ~sklearn.SklearnPredictor
