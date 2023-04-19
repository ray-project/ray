.. _batch_inference_home:

Running Batch Inference with Ray
================================

Batch inference refers to generating model predictions on a set of input data.
The model can range from a simple a Python function to a complex neural network.
In batch inference, also known as offline inference, your model is run on a large
batch of data on demand.
This is in contrast to online inference, where the model is run immediately on a data point when it becomes available.
Here's a simple schematic of batch inference, "mapping" batches to predictions via model inference:

.. figure:: images/batch_inference.png

  Evaluating a batch of input data with a model to get predictions.

Batch inference is a foundational workload for many AI companies, especially since more and more pre-trained models become available.
And while batch inference looks simple at the surface, it can be challenging to do right in production.
For instance, your data batches can be excessively large, too slow to process sequentially, or might need custom preprocessing before being fed into your models.
To run inference workloads effectively at scale, you need to

- manage your compute infrastructure and cloud clusters
- parallelize data processing and utilize all your cluster resources (CPUs & GPUs)
- efficiently transferring data between cloud storage, CPUs for preprocessing, and GPUs for model inference.

Here's a more realistic view of batch inference for modern AI applications:

.. figure:: images/batch_inference_overview.png

  Evaluating a batch of input data with a model to get predictions.

Why use Ray for batch inference?
---------------------------------

1. **Faster and Cheaper for modern Deep Learning Applications**: Ray is built for complex workloads and supports loading and preprocessing data with CPUs and model inference on GPUs.
2. **Cloud, framework, and data format agnostic**. Ray Data works on any cloud provider, any ML framework (like PyTorch and Tensorflow) and does not require a particular file format.
3. **Out of the box scaling**: The same code that works on one machine also runs on a large cluster without any changes.
4. **Python first** You can express your inference job directly in Python instead of YAML files or other formats.

Quick Start
-----------

To get started with batch inference with Ray, the first thing you need to do is
install Ray together with its data processing library Ray Data:

.. code-block:: bash

    pip install ray[data]

Running batch inference is conceptually easy and requires just three simple steps:

1. Load your data into a Ray Dataset and apply any preprocessing you need.
2. Define your model for inference.
3. Run inference on your data by using the ``.map_batches()`` method from Ray Data.

The last step also defines how your batch processing job gets distributed across your (local) cluster.
We start with very simple use cases here and build up to more complex ones in other guides and tutorials.
But essentially all complications boil down to extensions of the above three steps,
like loading and storing data from cloud storage, using complex preprocessing functions,
demanding model setups, additional postprocessing, or other customizations.

1. Loading and Preprocessing Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this quick-start guide we'll simply use very small, in-memory data sets by
leveraging common Python libraries like NumPy and Pandas.
For larger data sets, you can use Ray Data to load data from cloud storage like S3 or GCS.
In general, once you loaded your datasets using Ray Data, you also want to apply some preprocessing steps.
We skip this step here for simplicity.
In any case, the result of this step is a Ray Datastream ``ds`` that we can use to run inference on.

.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_quickstart_load_start__
            :end-before: __hf_quickstart_load_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_quickstart_load_start__
            :end-before: __pt_quickstart_load_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_quickstart_load_start__
            :end-before: __tf_quickstart_load_end__

2. Setting up your Model
~~~~~~~~~~~~~~~~~~~~~~~~

Next, you want to define your model for inference.
Below you find easy examples for HuggingFace, PyTorch, and TensorFlow.
The core idea is to define a "predictor" class that loads your model in its ``__init__`` method and
and implements a ``__call__`` method that takes a batch of data and returns a batch of predictions.
It's important to understand that Ray Data has different batch modes,
depending on the type of data you're processing and how you loaded it in the first place.
For this quick-start guide, we consider this part an implementation detail and just focus on the models themselves.

.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_quickstart_model_start__
            :end-before: __hf_quickstart_model_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_quickstart_model_start__
            :end-before: __pt_quickstart_model_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_quickstart_model_start__
            :end-before: __tf_quickstart_model_end__


3. Getting Predictions with Ray Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have your Ray Datastream ``ds`` and your predictor class, you can finally use
``ds.map_batches(...)`` to get predictions.
Mapping batches this way is the recommended way to run inference with Ray, but there are also other options.
``map_batches`` takes your predictor class as an argument and allows you to specify
``compute`` resources by defining a so-called ``ActorPoolStrategy``.
In the example below, we use 2 CPUs to run inference in parallel and then print the results.
We cover resource allocation in more detail in other parts of this guide.

.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_quickstart_prediction_start__
            :end-before: __hf_quickstart_prediction_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_quickstart_prediction_start__
            :end-before: __pt_quickstart_prediction_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_quickstart_prediction_start__
            :end-before: __tf_quickstart_prediction_end__

Learn more
----------

Batch inference is just one small part of the Machine Learning workflow, and only
a fraction of what Ray can do.

.. figure:: images/train_predict_pipeline.png

  How batch inference fits into the bigger picture of training and prediction AI models.

To learn more about Ray and batch inference, check out the following resources:

.. panels::
    :container: container pb-3
    :column: col-md-3 px-1 py-1
    :img-top-cls: p-2 w-75 d-block mx-auto fixed-height-img

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: https://github.com/ray-project/ray-educational-materials/blob/main/Computer_vision_workloads/Semantic_segmentation/Scaling_batch_inference.ipynb
        :type: url
        :text: [Tutorial] Architectures for Scalable Batch Inference with Ray
        :classes: btn-link btn-block stretched-link scalableBatchInference
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /ray-core/examples/batch_prediction
        :type: ref
        :text: [Example] Batch Prediction using Ray Core
        :classes: btn-link btn-block stretched-link batchCore
    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/nyc_taxi_basic_processing
        :type: ref
        :text: [Example] Batch Inference on NYC taxi data using Ray Data
        :classes: btn-link btn-block stretched-link nycTaxiData

    ---
    :img-top: /images/ray_logo.png

    .. link-button:: /data/examples/ocr_example
        :type: ref
        :text: [Example] Batch OCR processing using Ray Data
        :classes: btn-link btn-block stretched-link batchOcr
