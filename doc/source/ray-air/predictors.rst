.. _air-predictors:

Inference with trained models
=============================

.. image:: images/air-predictor.png

After you train a model, you will often want to use the model to do inference/prediction.

Ray AIR Predictors are a class that loads models from Checkpoints to perform inference. Predictors are used by Batch Predictors and PredictorDeployments to do large-scale scoring and online inference.

Predictors Basics
-----------------

Let's walk through a basic usage of the Predictor. In the below example, we create Checkpoint object from a model definition. Then, the checkpoint is used to create a framework specific Predictor (the TensorflowPredictor), which then can be used for inference:

.. literalinclude:: doc_code/use_pretrained_model.py
    :language: python
    :start-after: __use_predictor_start__
    :end-before: __use_predictor_end__



Predictors expose a ``predict`` method that accepts an input batch of type ``DataBatchType`` (which is a typing union of different standard Python ecosystem data types, such as Pandas Dataframe or Numpy Array) and outputs predictions of the same type as the input batch.

**Life of a prediction:** Underneath the hood, when the ``Predictor.predict`` method is called the following occurs:

- The input batch is converted into a pandas DataFrame. Tensor input (like a ``np.ndarray``) will be converted into a single column Pandas Dataframe.
- If there is a :ref:`Preprocessor <air-preprocessor-ref>` saved in the provided :ref:`Checkpoint <air-checkpoint-ref>`, the preprocessor will be used to transform the DataFrame.
- The transformed DataFrame will be passed to the model for inference.
- The predictions will be outputted by ``predict`` in the same type as the original input.

.. TODO: What about GPU inference


Creating a Predictor
--------------------
Predictors can be created from Checkpoints.

Either as a result of Training (Result, ResultGrid) or from a :ref:`pretrained model <use-pretrained-model>`.

Checkpoints contain the trained model for prediction and the fitted Preprocessor

Link to Checkpoint docs when they exist.

Code snippet showing from_checkpoint.

Using Predictors
------------------------
Predictors load models from checkpoints to perform inference.

Predictors expose a ``predict`` method that accepts an input batch of type
    ``DataBatchType`` and outputs predictions of the same type as the input batch.

When the ``predict`` method is called the following occurs:

        - The input batch is converted into a pandas DataFrame. Tensor input (like a
          ``np.ndarray``) will be converted into a single column Pandas Dataframe.
        - If there is a :ref:`Preprocessor <air-preprocessor-ref>` saved in the provided
          :ref:`Checkpoint <air-checkpoint-ref>`, the preprocessor will be used to
          transform the DataFrame.
        - The transformed DataFrame will be passed to the model for inference (via the
          ``predictor._predict_pandas`` method).
        - The predictions will be outputted by ``predict`` in the same type as the
          original input.

There are three ways to do prediction.

.. _air-predictor-standalone:

1: Standalone for development/debugging
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Show how to pass in a single batch of data.
Make sure that your Predictor works with your model/checkpoint.

2: Offline Batch Prediction
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Link to batch prediction guide

3: Online Prediction with Ray Serve
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Link to serving guide


Examples
--------

Non Deep Learning
~~~~~~~~~~~~~~~~~
Show tabs for xgboost and lightgbm

Link to full batch prediction example
Link to full online serving example

Deep Learning
~~~~~~~~~~~~~
Show tabs for Torch/Tf for each

Single tensor (e.g. Image Data)
###############################
Link to full batch prediction example
Link to full online serving

Tabular
#######
Link to full batch prediction example
Link to full online serving example

Multi-modal
###########
Link to full batch prediction example
Link to full online serving example

Developer Guide: Implementing your own Predictor
------------------------------------------------
    To implement a new Predictor for your particular framework, you should subclass
    the base ``Predictor`` and implement the following two methods:

        1. ``_predict_pandas``: Given a pandas.DataFrame input, return a
            pandas.DataFrame containing predictions.
        2. ``from_checkpoint``: Logic for creating a Predictor from an
           :ref:`AIR Checkpoint <air-checkpoint-ref>`.
        3. Optionally ``_predict_arrow`` for better performance when working with
           tensor data to avoid extra copies from Pandas conversions.


