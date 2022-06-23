.. _air-predictors:

Inference with trained models
=============================

.. image:: images/air-predictors.png

After you train a model, you will often want to use the model to do inference and prediction.

Ray AIR Predictors are a class that loads models from :class:`Checkpoints` to perform inference. 
Predictors are used by `BatchPredictors` and `PredictorDeployments` to do large-scale scoring or online inference.

Predictors Basics
-----------------

Let's walk through a basic usage of the Predictor. In the below example, we create `Checkpoint` object from a model definition. 
Checkpoints can be generated from a variety of different ways -- 
see the Checkpoints user guide for more details.
.. TODO - link to Checkpoint user guide

The checkpoint then is used to create a framework specific Predictor (in our example, a `TensorflowPredictor`), which then can be used for inference:

.. literalinclude:: doc_code/use_pretrained_model.py
    :language: python
    :start-after: __use_predictor_start__
    :end-before: __use_predictor_end__


Predictors expose a ``predict`` method that accepts an input batch of type ``DataBatchType`` (which is a typing union of different standard Python ecosystem data types, such as Pandas Dataframe or Numpy Array) and outputs predictions of the same type as the input batch.

**Life of a prediction:** Underneath the hood, when the ``Predictor.predict`` method is called the following occurs:

- The input batch is converted into a Pandas DataFrame. Tensor input (like a ``np.ndarray``) will be converted into a single column Pandas Dataframe.
- If there is a :ref:`Preprocessor <air-preprocessor-ref>` saved in the provided :ref:`Checkpoint <air-checkpoint-ref>`, the preprocessor will be used to transform the DataFrame.
- The transformed DataFrame will be passed to the model for inference.
- The predictions will be outputted by ``predict`` in the same type as the original input.


Batch Prediction
----------------

Ray AIR provides a ``BatchPredictor`` utility for large-scale batch inference.

The BatchPredictor takes in a checkpoint and a predictor class and executes 
large-scale batch prediction on a given dataset in a parallel/distributed fashion when calling ``predict()``.

``predict()`` will load the entire given dataset into memory, which may be a problem if your dataset
size is larger than your available cluster memory. See the :ref:`pipelined-prediction` section for more details.

.. literalinclude:: doc_code/use_pretrained_model.py
    :language: python
    :start-after: __batch_prediction_start__
    :end-before: __batch_prediction_end__

Below, we provide examples of using common frameworks to do batch inference for different data types:

**Tabular**

.. tabbed:: XGBoost

    .. literalinclude:: doc_code/xgboost_starter.py
        :language: python
        :start-after: __air_xgb_batchpred_start__
        :end-before: __air_xgb_batchpred_end__


    .. todo - include py files as orphans so that we can do a versioned link?
    .. See the full script here: `Code <doc_code/xgboost_starter.py>`_.

.. tabbed:: Pytorch

    .. literalinclude:: doc_code/pytorch_tabular_starter.py
        :language: python
        :start-after: __air_pytorch_batchpred_start__
        :end-before: __air_pytorch_batchpred_end__

    .. TODO: include py files as orphans so that we can do a versioned link?


.. tabbed:: Tensorflow

    Coming soon!
    .. TODO: include py files as orphans so that we can do a versioned link?

**Image**

.. tabbed:: Torch

    .. literalinclude:: doc_code/torch_image_batch_pretrained.py
        :language: python
        :start-after: __batch_prediction_start__
        :end-before: __batch_prediction_end__

.. tabbed:: TensorFlow

    .. literalinclude:: doc_code/tf_image_batch_pretrained.py
        :language: python
        :start-after: __batch_prediction_start__
        :end-before: __batch_prediction_end__

**Text**

Coming soon!

.. _pipelined-prediction:

Lazy/Pipelined Prediction
~~~~~~~~~~~~~~~~~~~~~~~~~
If you have a large dataset but not a lot of available memory, you can use the 
:method:`predict_pipelined <ray.train.batch_predictor.BatchPredictor.predict_pipelined>` method.

Unlike :py:method:`predict` which will load the entire data into memory, ``predict_pipelined`` will create a 
:class:`DatasetPipeline`` object, which will *lazily* load the data and perform inference on a smaller batch of data at a time.

The lazy loading of the data will allow you to operate on datasets much greater than your available memory.
Execution can be triggered by pulling from the pipeline, as shown in the example below.


.. literalinclude:: doc_code/use_pretrained_model.py
    :language: python
    :start-after: __pipelined_prediction_start__
    :end-before: __pipelined_prediction_end__


Online inference
----------------

Check out the :ref:`air-serving-guide` for details on how to perform online inference with AIR.


Developer Guide: Implementing your own Predictor
------------------------------------------------
To implement a new Predictor for your particular framework, you should subclass the base ``Predictor`` and implement the following two methods:

1. ``_predict_pandas``: Given a pandas.DataFrame input, return a pandas.DataFrame containing predictions.
2. ``from_checkpoint``: Logic for creating a Predictor from an :ref:`AIR Checkpoint <air-checkpoint-ref>`.
3. Optionally ``_predict_arrow`` for better performance when working with tensor data to avoid extra copies from Pandas conversions.


