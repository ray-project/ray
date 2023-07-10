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


.. _batch-prediction:

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


Configuring Batch Prediction
----------------------------
To configure the computation resources for your `BatchPredictor`, you have to set the following parameters in `predict()`:

- `min_scoring_workers` and `max_scoring_workers`

  - The BatchPredictor will internally create an actor pool to autoscale the number of workers from [min, max] to execute your transforms.

  - If not set, the auto-scaling range will be set to [1, inf) by default.

- `num_gpus_per_worker`:

  - If you want to use GPU for batch prediction, please set this parameter explicitly.

  - If not specified, the BatchPredictor will perform inference on CPUs by default.

- `num_cpus_per_worker`:

  - Set the number of CPUs for a worker.

- `separate_gpu_stage`:

  - If using GPUs, whether to use separate stages for GPU inference and data preprocessing.

  - Enabled by default to avoid excessive preprocessing workload on GPU workers. You may disable it if your preprocessor is very lightweight.

Here are some examples:

**1. Use multiple CPUs for Batch Prediction:**

- If `num_gpus_per_worker` not specified, use CPUs for batch prediction by default.

- Two workers with 3 CPUs each.

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __configure_batch_predictor_cpu_only_start__
    :end-before: __configure_batch_predictor_cpu_only_end__

**2. Use multiple GPUs for Batch prediction:**

- Two workers, each with 1 GPU and 1 CPU (by default).

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __configure_batch_predictor_gpu_only_start__
    :end-before: __configure_batch_predictor_gpu_only_end__

**3. Configure Auto-scaling:**

- Scale from 1 to 4 workers, depending on your dataset size and cluster resources.

- If no min/max values are provided, `BatchPredictor` will scale from 1 to inf workers by default.

.. literalinclude:: doc_code/predictors.py
    :language: python
    :start-after: __configure_batch_predictor_scaling_start__
    :end-before: __configure_batch_predictor_scaling_end__


Developer Guide: Implementing your own Predictor
------------------------------------------------

If you're using an unsupported framework, or if built-in predictors are too inflexible,
you may need to implement a custom predictor.

To implement a custom :class:`~ray.train.predictor.Predictor`,
subclass :class:`~ray.train.predictor.Predictor` and implement:

* :meth:`~ray.train.predictor.Predictor.__init__`
* :meth:`~ray.train.predictor.Predictor._predict_numpy` or :meth:`~ray.train.predictor.Predictor._predict_pandas`
* :meth:`~ray.train.predictor.Predictor.from_checkpoint`

.. tip::
    You don't need to implement both
    :meth:`~ray.train.predictor.Predictor._predict_numpy` and
    :meth:`~ray.train.predictor.Predictor._predict_pandas`. Pick the method that's
    easiest to implement. If both are implemented, override
    :meth:`~ray.train.predictor.Predictor.preferred_batch_format` to specify which format
    is more performant. This allows upstream producers to choose the best format.

Examples
~~~~~~~~

We'll walk through how to implement a predictor for ``statsmodel`` --
a Python library that provides regression and linear models.

For more examples, read the source code of built-in predictors like
:class:`~ray.train.torch.TorchPredictor`,
:class:`~ray.train.xgboost.XGBoostPredictor`, and
:class:`~ray.train.sklearn.SklearnPredictor`.

Before you begin
****************

First, install statsmodel and Ray AIR.

.. code-block:: console

    pip install statsmodel 'ray[air]'

Then, import the objects required for this example.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_imports_start__
    :end-before: __statsmodelpredictor_imports_end__

Finally, create a stub the `StatsmodelPredictor` class.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_signature_start__
    :end-before: __statsmodelpredictor_signature_end__

Create a model
**************

You'll need to pass a model to the ``StatsmodelPredictor`` constructor.

To create the model, fit a linear model on the
`Guerry dataset <https://vincentarelbundock.github.io/Rdatasets/doc/HistData/Guerry.html>`_.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_model_start__
    :end-before: __statsmodelpredictor_model_end__


Implement `__init__`
********************

Use the constructor to set instance attributes required for prediction. In
the code snippet below, we assign the fitted model to an attribute named
``results``.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_init_start__
    :end-before: __statsmodelpredictor_init_end__

.. warning::
    You must call the base class' constructor; otherwise,
    `Predictor.predict <ray.train.predictor.Predict.predict>` raises a
    ``NotImplementedError``.

Implement `from_checkpoint`
***************************

:meth:`~ray.train.predictor.from_checkpoint` creates a
:class:`~ray.train.predictor.Predictor` from a
:class:`~ray.air.checkpoint.Checkpoint`.

Before implementing :meth:`~ray.train.predictor.from_checkpoint`,
save the fitten model to a directory, and create a
:class:`~ray.air.checkpoint.Checkpoint` from that directory.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_checkpoint_start__
    :end-before: __statsmodelpredictor_checkpoint_end__

Then, implement :meth:`~ray.train.predictor.from_checkpoint`.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_from_checkpoint_start__
    :end-before: __statsmodelpredictor_from_checkpoint_end__

Implement `_predict_numpy` or `_predict_pandas`
***********************************************

Because your OLS model accepts dataframes as input, you should implement
:meth:`~ray.train.predictor.Predictor._predict_pandas`.

:meth:`~ray.train.predictor.Predictor._predict_pandas` performs inference on a
batch of pandas data. It accepts a ``pandas.DataFrame`` as input and return a
``pandas.DataFrame`` as output.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_predict_pandas_start__
    :end-before: __statsmodelpredictor_predict_pandas_end__


Perform inference
*****************

To perform inference with the completed ``StatsmodelPredictor``:

1. Create a :class:`~ray.train.batch_predictor.BatchPredictor` from your
   checkpoint.
2. Read the Guerry dataset into a :class:`~ray.data.Dataset`.
3. Call :class:`~ray.train.batch_predictor.BatchPredictor.predict` to perform
   regression on the samples in the dataset.

.. literalinclude:: doc_code/statsmodel_predictor.py
    :language: python
    :dedent:
    :start-after: __statsmodelpredictor_predict_start__
    :end-before: __statsmodelpredictor_predict_end__


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
