.. _air-key-concepts:

Key Concepts
============

Here, we cover the main concepts in AIR.

.. contents::
    :local:


Datasets
--------

:ref:`Ray Datasets <datasets>` are the standard way to load and exchange data in Ray AIR. In AIR, Datasets are used extensively for data loading, preprocessing, and batch inference.


Preprocessors
-------------

Preprocessors are primitives that can be used to transform input data into features. Preprocessors operate on :ref:`Datasets <datasets>`, which makes them scalable and compatible with a variety of datasources and dataframe libraries.

A Preprocessor is fitted during Training, and applied at runtime in both Training and Serving on data batches in the same way. AIR comes with a collection of built-in preprocessors, and you can also define your own with simple templates.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_preprocessors_start__
    :end-before: __air_preprocessors_end__


Trainers
--------

Trainers are wrapper classes around third-party training frameworks such as XGBoost and Pytorch. They are built to help integrate with core Ray actors (for distribution), Ray Tune, and Ray Datasets.

See the documentation on :ref:`Trainers <air-trainers>`.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_trainer_start__
    :end-before: __air_trainer_end__

Trainer objects produce a :ref:`Result <air-results-ref>` object after calling ``.fit()``.
These objects contain training metrics as well as checkpoints to retrieve the best model.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_trainer_output_start__
    :end-before: __air_trainer_output_end__


Tuner
-----

:ref:`Tuners <air-tuner-ref>` offer scalable hyperparameter tuning as part of :ref:`Ray Tune <tune-main>`.

Tuners can work seamlessly with any Trainer but also can support arbitrary training functions.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_tuner_start__
    :end-before: __air_tuner_end__

.. _air-checkpoints-doc:

Checkpoints
-----------

The AIR trainers, tuners, and custom pretrained model generate :class:`a framework-specific Checkpoint<ray.air.Checkpoint>` object.
Checkpoints are a common interface for models that are used across different AIR components and libraries.

There are two main ways to generate a checkpoint.

Checkpoint objects can be retrieved from the Result object returned by a Trainer or Tuner ``.fit()`` call.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_checkpoints_start__
    :end-before: __air_checkpoints_end__

You can also generate a checkpoint from a pretrained model. Each AIR supported machine learning (ML) framework has
a ``Checkpoint`` object that can be used to generate an AIR checkpoint:

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __checkpoint_adhoc_start__
    :end-before: __checkpoint_adhoc_end__


Checkpoints can be used to instantiate a :class:`Predictor`, :class:`BatchPredictor`, or :class:`PredictorDeployment` classes,
as seen below.


Batch Predictor
---------------

You can take a checkpoint and do batch inference using the BatchPredictor object.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_batch_predictor_start__
    :end-before: __air_batch_predictor_end__

.. _air-key-concepts-online-inference:

Deployments
-----------

Deploy the model as an inference service by using Ray Serve and the ``PredictorDeployment`` class.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_deploy_start__
    :end-before: __air_deploy_end__

After deploying the service, you can send requests to it.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_inference_start__
    :end-before: __air_inference_end__
