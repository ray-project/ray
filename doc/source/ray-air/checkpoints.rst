.. _air-checkpoints-doc:

Using Checkpoints
=================

The AIR trainers, tuners, and custom pretrained model generate Checkpoints. An AIR Checkpoint is a common format for models that
are used across different components of the Ray AI Runtime. This common format allow easy interoperability among AIR components
and seamless integration with external supported machine learning frameworks.

.. image:: images/checkpoints.jpg

What is a checkpoint?
---------------------

A Checkpoint object is a serializable reference to a model. A model can be represented in one of three ways:

- as a directory on local (on-disk) storage
- as a directory on an external storage (e.g., cloud storage)
- as an in-memory dictionary

Because of these different model storage representation, Checkpoint models provide useful flexibility in
distributed environments, where you want to recreate an instance of the same model on multiple nodes or
across different Ray clusters.

How to create a checkpoint?
---------------------------

There are two ways to generate a checkpoint.

The first way is to generate it from a pretrained model. Each AIR supported machine learning (ML) framework has
a ``Checkpoint`` method that can be used to generate an AIR checkpoint:

.. literalinclude:: doc_code/checkpoint_usage.py
    :language: python
    :start-after: __checkpoint_quick_start__
    :end-before: __checkpoint_quick_end__


Another way is to retrieve it from the result object returned by a Trainer or Tuner.

.. literalinclude:: doc_code/checkpoint_usage.py
    :language: python
    :start-after: __use_trainer_checkpoint_start__
    :end-before: __use_trainer_checkpoint_end__

How to use a checkpoint?
------------------------

Checkpoints can be used to instantiate a :class:`Predictor`, :class:`BatchPredictor`, or :class:`PredictorDeployment` class.
An instance of this instantiated class (in memory) can be used for inference.

For instance, the code example below shows how a checkpoint in the :class:`BatchPredictor` is used for scalable batch inference:

.. literalinclude:: doc_code/checkpoint_usage.py
    :language: python
    :start-after: __batch_pred_start__
    :end-before: __batch_pred_end__

Another example below demonstrates how to use a checkpoint for an online inference via :class:`PredictorDeployment`:

.. literalinclude:: doc_code/checkpoint_usage.py
    :language: python
    :start-after: __online_inference_start__
    :end-before: __online_inference_end__

Furthermore, a Checkpoint object has methods to translate between different checkpoint storage locations.
With this flexibility, Checkpoint objects can be serialized and used in different contexts
(e.g., on a different process or a different machine):

.. literalinclude:: doc_code/checkpoint_usage.py
    :language: python
    :start-after: __basic_checkpoint_start__
    :end-before: __basic_checkpoint_end__


Example: Using Checkpoints with MLflow
--------------------------------------

`MLflow <https://mlflow.org/>`__ has its own `checkpoint format <https://www.mlflow.org/docs/latest/models.html>`__ called
the "MLflow Model." It is a standard format to package machine learning models that can be used in a variety of downstream tools.

Below is an example of using MLflow models as a Ray AIR Checkpoint.

.. literalinclude:: doc_code/checkpoint_mlflow.py
    :language: python
    :start-after: __mlflow_checkpoint_start__
    :end-before: __mlflow_checkpoint_end__


