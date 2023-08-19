.. _preprocessor-ref:

Preprocessor
============

Preprocessor Interface
------------------------

.. currentmodule:: ray.data

Constructor
~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~preprocessor.Preprocessor

Fit/Transform APIs
~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: doc/

    ~preprocessor.Preprocessor.fit
    ~preprocessor.Preprocessor.fit_transform
    ~preprocessor.Preprocessor.transform
    ~preprocessor.Preprocessor.transform_batch
    ~preprocessor.Preprocessor.transform_stats


Generic Preprocessors
---------------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.BatchMapper
    ~preprocessors.Chain
    ~preprocessors.Concatenator
    ~preprocessors.SimpleImputer

Categorical Encoders
--------------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.Categorizer
    ~preprocessors.LabelEncoder
    ~preprocessors.MultiHotEncoder
    ~preprocessors.OneHotEncoder
    ~preprocessors.OrdinalEncoder

Feature Scalers
---------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.MaxAbsScaler
    ~preprocessors.MinMaxScaler
    ~preprocessors.Normalizer
    ~preprocessors.PowerTransformer
    ~preprocessors.RobustScaler
    ~preprocessors.StandardScaler

K-Bins Discretizers
-------------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.CustomKBinsDiscretizer
    ~preprocessors.UniformKBinsDiscretizer

Image Preprocessors
-------------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.TorchVisionPreprocessor

Text Encoders
-------------

.. autosummary::
    :toctree: doc/

    ~preprocessors.CountVectorizer
    ~preprocessors.FeatureHasher
    ~preprocessors.HashingVectorizer
    ~preprocessors.Tokenizer
