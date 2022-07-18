.. _air-key-concepts:

Key Concepts
============

Here, we cover the main concepts in AIR.

.. contents::
    :local:


Preprocessors
-------------

Preprocessors are primitives that can be used to transform input data into features.

A preprocessor can be fitted during Training, and applied at runtime in both Training and Serving on data batches in the same way. AIR comes with a collection of built-in preprocessors, and you can also define your own with simple templates.

Preprocessors operate on :ref:`Ray Datasets <datasets>`, which makes them scalable and compatible with a variety of datasources and dataframe libraries.


.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_preprocessors_start__
    :end-before: __air_preprocessors_end__


Trainers
--------

Trainers are wrapper classes around third-party training frameworks like XGBoost and Pytorch. They are built to help integrate with core Ray actors (for distribution), Ray Tune, and Ray Datasets.

See the documentation on :ref:`Trainers <air-trainer-ref>`.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_trainer_start__
    :end-before: __air_trainer_end__



Trainer objects will produce a :ref:`Result <air-results-ref>` object after calling ``.fit()``.  These objects will contain training metrics as long as checkpoints to retrieve the best model.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_trainer_output_start__
    :end-before: __air_trainer_output_end__

.. _air-session-key-concepts:

Session
-------

Ray AIR exposes a functional API for users to define training behavior, or for developers to create their own ``Trainer``\s.
In both cases, there is a need for the following interactions:

1. To disseminate information downstream, including ``trial_name``, ``trial_id``, ``trial_resources``, rank information etc.
2. To report information to upstream, including metrics and checkpoint.

To facilitate such interactions, we introduce the :ref:`Session <air-session-ref>` concept.

The session concept exists on several levels: The execution layer (called `Tune Session`) and the Data Parallel training layer
(called `Train Session`).
The following figure shows how these two sessions look like in a Data Parallel training scenario.

.. image:: images/session.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1g0pv8gqgG29aPEPTcd4BC0LaRNbW1sAkv3H6W1TCp0c/edit


Tuner
-----

:ref:`Tuners <air-tuner-ref>` offer scalable hyperparameter tuning as part of :ref:`Ray Tune <tune-main>`.

Tuners can work seamlessly with any Trainer but also can support arbitrary training functions.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_tuner_start__
    :end-before: __air_tuner_end__



Batch Predictor
---------------

You can take a trained model and do batch inference using the BatchPredictor object.

.. literalinclude:: doc_code/air_key_concepts.py
    :language: python
    :start-after: __air_batch_predictor_start__
    :end-before: __air_batch_predictor_end__

.. _air-key-concepts-online-inference:

Online Inference
----------------

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
