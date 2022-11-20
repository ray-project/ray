.. _air-predictors:

Using Predictors for Inference
==============================

.. tip::
   Refer to the blog on `Model Batch Inference in Ray <https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets>`__
   for an overview of batch inference strategies in Ray and additional examples.

.. https://docs.google.com/presentation/d/1jfkQk0tGqgkLgl10vp4-xjcbYG9EEtlZV_Vnve_NenQ/edit

.. image:: images/predictors.png


After you train a model, you will often want to use the model to do inference and prediction.
To do so, you can use a Ray AIR Predictor. In this guide, we'll cover how to use the Predictor
on different types of data.


What are predictors?
--------------------

Ray AIR Predictors are a class that loads models from `Checkpoint` to perform inference.

Predictors are used by `BatchPredictor` and `PredictorDeployment` to do large-scale scoring or online inference.

Let's walk through a basic usage of the Predictor. In the below example, we create `Checkpoint` object from a model definition.
Checkpoints can be generated from a variety of different ways -- see the :ref:`Checkpoints <air-checkpoints-doc>` user guide for more details.

The checkpoint then is used to create a framework specific Predictor (in our example, a `TensorflowPredictor`), which then can be used for inference:

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __use_predictor_start__
    :end-before: __use_predictor_end__


Predictors expose a ``predict`` method that accepts an input batch of type ``DataBatchType`` (which is a typing union of different standard Python ecosystem data types, such as Pandas Dataframe or Numpy Array) and outputs predictions of the same type as the input batch.

**Life of a prediction:** Underneath the hood, when the ``Predictor.predict`` method is called the following occurs:

- The input batch is converted into a Pandas DataFrame. Tensor input (like a ``np.ndarray``) will be converted into a single-column Pandas Dataframe.
- If there is a :ref:`Preprocessor <air-preprocessor-ref>` saved in the provided :ref:`Checkpoint <air-checkpoint-ref>`, the preprocessor will be used to transform the DataFrame.
- The transformed DataFrame will be passed to the model for inference.
- The predictions will be outputted by ``predict`` in the same type as the original input.


Batch Prediction
----------------

Ray AIR provides a ``BatchPredictor`` utility for large-scale batch inference.

The BatchPredictor takes in a checkpoint and a predictor class and executes
large-scale batch prediction on a given dataset in a parallel/distributed fashion when calling ``predict()``.

.. note::
    ``predict()`` will load the entire given dataset into memory, which may be a problem if your dataset
    size is larger than your available cluster memory. See the :ref:`pipelined-prediction` section for a workaround.

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __batch_prediction_start__
    :end-before: __batch_prediction_end__


Additionally, you can compute metrics from the predictions. Do this by:

1. specifying a function for computing metrics
2. using `keep_columns` to keep the label column in the returned dataset
3. using `map_batches` to compute metrics on a batch-by-batch basis
4. Aggregate batch metrics via `mean()`

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __compute_accuracy_start__
    :end-before: __compute_accuracy_end__

Batch Inference Examples
------------------------
Below, we provide examples of using common frameworks to do batch inference for different data types:

Tabular
~~~~~~~

.. tabbed:: XGBoost

    .. literalinclude:: examples/xgboost_batch_prediction.py
        :language: python

.. tabbed:: Pytorch

    .. literalinclude:: examples/pytorch_tabular_batch_prediction.py
        :language: python

.. tabbed:: Tensorflow

    .. literalinclude:: examples/tf_tabular_batch_prediction.py
        :language: python


Image
~~~~~

.. tabbed:: Pytorch

    .. literalinclude:: examples/torch_image_batch_pretrained.py
        :language: python


.. tabbed:: Tensorflow

    Coming soon!

Text
~~~~

Coming soon!

.. _pipelined-prediction:

Lazy/Pipelined Prediction (experimental)
----------------------------------------

If you have a large dataset but not a lot of available memory, you can use the
:meth:`predict_pipelined <ray.train.batch_predictor.BatchPredictor.predict_pipelined>` method.

Unlike :py:meth:`predict` which will load the entire data into memory, ``predict_pipelined`` will create a
:class:`DatasetPipeline` object, which will *lazily* load the data and perform inference on a smaller batch of data at a time.

The lazy loading of the data will allow you to operate on datasets much greater than your available memory.
Execution can be triggered by pulling from the pipeline, as shown in the example below.


.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __pipelined_prediction_start__
    :end-before: __pipelined_prediction_end__


Online Inference
----------------

Check out the :ref:`air-serving-guide` for details on how to perform online inference with AIR.


Developer Guide: Implementing your own Predictor
------------------------------------------------
To implement a new Predictor for your particular framework, you should subclass the base ``Predictor`` and implement the following two methods:

1. ``_predict_pandas``: Given a pandas.DataFrame input, return a pandas.DataFrame containing predictions.
2. ``from_checkpoint``: Logic for creating a Predictor from an :ref:`AIR Checkpoint <air-checkpoint-ref>`.
3. Optionally ``_predict_numpy`` for better performance when working with tensor data to avoid extra copies from Pandas conversions.
