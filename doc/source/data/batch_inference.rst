.. _batch_inference_home:

End-to-end: Offline Batch Inference
===================================

.. tip::

    [Get in touch with us](https://forms.gle/sGX7PQhheBGL6yxQ6) if you are interested in Ray Data for offline batch inference. We believe we have the fastest & cheapest solution out there and would be happy to work with you on a POC.

Offline batch inference is a process for generating model predictions on a fixed set of input data. Ray Data offers an efficient and scalable solution for batch inference, providing faster execution and cost-effectiveness for deep learning applications.

.. figure:: images/batch_inference.png


Why choose Ray Data for offline inference?
------------------------------------------

.. dropdown:: Faster and Cheaper for modern Deep Learning Applications

    Ray Data is built for deep learning applications that involve both CPU preprocessing and GPU inference. Ray Data streams the working data from CPU preprocessing tasks to GPU inferencing tasks, allowing both sets of resources to be utilized concurrently.

    By using Ray Data your GPUs are no longer idle during CPU computation, reducing overall cost of the batch inference job.

.. dropdown:: Cloud, framework, and data format agnostic

    Ray Data has no restrictions on cloud provider, ML framework, or data format.
    
    Through the :ref:`Ray cluster launcher <cluster-index>`, you can start a Ray cluster on any of the common cloud providers. Since Ray Data works with any Python function or class, you can use any ML framework of your choice, including PyTorch, HuggingFace, or Tensorflow. Ray Data also does not require a particular file format, and supports a :ref:`wide variety of formats <loading_data>` including CSV, Parquet, raw images, etc.

.. dropdown:: Out of the box scaling

    Ray Data is built on Ray, so it easily scales to many machines. The same code that works on one machine also runs on a large cluster without any changes.

.. dropdown:: Python first

    With Ray Data, you can express your inference job directly in Python instead of
    YAML files or other formats. This allows for faster iterations, easier debugging, and more native developer experience.

Quickstart
----------
To start, install Ray with the data processing library, Ray Data:

.. code-block:: bash

    pip install ray[data]

Then, use Ray Data for offline inference in 4 simple steps:

1. Load in your data into a Ray Dataset. Many different data types and formats are supported, see :ref:`Loading Data <loading_data>` for more details.
2. Loading the pre-trained model in a Python class. 
3. Transform your dataset using the pre-trained model. See :ref:`Transforming Data <transforming-data>` for more details.
4. Get the final predictions by either iterating through the output or saving the results. See :ref:`Consuming data <consuming_data>` for more details.

See :ref:`below <batch_inference_examples>` for more in-depth examples for your use case.

.. tabs::

    .. group-tab:: HuggingFace
        
        .. testcode::
            
            import ray
            import numpy as np
            from typing import Dict

            # Step 1: Create a Ray Dataset from in-memoruy Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

            # Define a Predictor class for inference.
            # Use a class so the model can be initialized just once in `__init__`
            # and then re-used for inference across multiple batches.
            class HuggingFacePredictor:
                def __init__(self):
                    from transformers import pipeline
                    # Initialize a pre-trained GPT2 Huggingface pipeline.
                    self.model = pipeline("text-generation", model="gpt2")

                def __call__(self, batch: Dict[str, np.ndarray]):
                    predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
                    # `predictions` is a list of length-one lists. For example:
                    # [[{'generated_text': '...'}], ..., [{'generated_text': "..."}]]
                    batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
                    return batch

            scale = ray.data.ActorPoolStrategy(size=2)
            predictions = ds.map_batches(HuggingFacePredictor, compute=scale)
            predictions.show(limit=1)
        
        .. testoutput::


    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_super_quick_start__
            :end-before: __hf_super_quick_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_super_quick_start__
            :end-before: __pt_super_quick_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_super_quick_start__
            :end-before: __tf_super_quick_end__

.. _batch_inference_examples:

More Examples
-------------


Configuration & Troubleshooting
-------------------------------

.. _batch_inference_gpu:

Using GPUs for inference
~~~~~~~~~~~~~~~~~~~~~~~~

To use GPUs for inference, first update the callable class implementation to
move the model and data to and from Cuda device.
Here's a quick example for a PyTorch model:

.. code-block:: diff

    from torchvision.models import resnet18

    class TorchModel:
        def __init__(self):
            self.model = resnet18(pretrained=True)
    +       self.model = self.model.cuda()
            self.model.eval()

        def __call__(self, batch: Dict[str, np.ndarray]):
            torch_batch = torch.stack(batch["data"])
    +       torch_batch = torch_batch.cuda()
            with torch.inference_mode():
                prediction = self.model(torch_batch)
    -           return {"class": prediction.argmax(dim=1).detach().numpy()}
    +           return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}


Next, specify ``num_gpus=N`` in :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
to indicate that each inference worker should use ``N`` GPUs.

.. code-block:: diff

    predictions = ds.map_batches(
        TorchModel,
        compute=ray.data.ActorPoolStrategy(size=2),
    +   num_gpus=1
    )

Configuring Batch Size
~~~~~~~~~~~~~~~~~~~~~~

An important parameter to set for :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
is ``batch_size``, which controls the size of the batches provided to the function.
Here's a simple example of loading the IRIS dataset (which has Pandas format by default)
and processing it with a batch size of `10`:

.. literalinclude:: ./doc_code/batch_formats.py
  :language: python
  :start-after: __simple_map_function_start__
  :end-before: __simple_map_function_end__

Increasing ``batch_size`` can result in faster execution by better leveraging vectorized
operations and hardware, reducing batch slicing and concatenation overhead, and overall
saturation of CPUs or GPUs.
On the other hand, this will also result in higher memory utilization, which can
lead to out-of-memory (OOM) failures.
If encountering OOMs, decreasing your ``batch_size`` may help.

.. caution::
  The default ``batch_size`` of ``4096`` may be too large for datasets with large rows
  (e.g. tables with many columns or a collection of large images).

Handling OOM from heavy model memory usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's common for models to consume a large amount of heap memory. For example, if a model
uses 5GB of RAM when created / run, and a machine has 16GB of RAM total, then no more
than three of these models can be run at the same time. The default resource assignments
of one CPU per task/actor will likely lead to OutOfMemoryErrors from Ray in this situation.

Let's suppose our machine has 16GiB of RAM and 8 GPUs. To tell Ray to construct at most
3 of these actors per node, we can override the CPU or memory:

.. code-block:: python

    # Require 5 CPUs per actor (so at most 3 can fit per 16 CPU node).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_cpus=5)

Handling expensive CPU preprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO

How does Ray Data compare to X for offline inference?
-----------------------------------------------------

Case studies
------------
- 


.. note::

    In this tutorial you'll learn what batch inference is, why you might want to use
    Ray for it, and how to use Ray Data effectively for this task.
    If you are familiar with the basics of inference tasks, jump straight to
    code in the :ref:`quickstart section <batch_inference_quickstart>`, our detailed
    :ref:`walk-through<batch_inference_walk_through>`,
    or our :ref:`in-depth guide for PyTorch models<batch_inference_advanced_pytorch_example>`.

Batch inference refers to generating model predictions on a set of input data.
The model can range from a simple Python function to a complex neural network.
In batch inference, also known as offline inference, your model is run on a large
batch of data on demand.
This is in contrast to online inference, where the model is run immediately on a
data point when it becomes available.

Here's a simple schematic of batch inference for the computer vision task classifying
images as cats or docs, by "mapping" batches of input data to predictions
via ML model inference:

.. figure:: images/batch_inference.png

  Evaluating a batch of input data with a model to get predictions.

Batch inference is a foundational workload for many AI companies, especially since
more and more pre-trained models become available.
And while batch inference looks simple at the surface, it can be challenging to do right in production.
For instance, your data batches can be excessively large, too slow to process sequentially,
or might need custom preprocessing before being fed into your models.
To run inference workloads effectively at scale, you need to:

- manage your compute infrastructure and cloud clusters
- parallelize data processing and utilize all your cluster resources (CPUs and GPUs)
- efficiently transfer data between cloud storage, CPUs for preprocessing, and GPUs for model inference

Here's a realistic view of batch inference for modern AI applications:

.. figure:: images/batch_inference_overview.png

  Evaluating a batch of input data with a model to get predictions.

Why use Ray Data for batch inference?
-------------------------------------

There are reasons to use Ray for batch inference, even if your current
use case does not require scaling yet:

1. **Faster and Cheaper for modern Deep Learning Applications**:
    Ray is built for
    complex workloads and supports loading and preprocessing data with CPUs and model inference on GPUs.
2. **Cloud, framework, and data format agnostic**:
    Ray Data works on any cloud provider or
    any ML framework (like PyTorch and Tensorflow) and does not require a particular file format.
3. **Out of the box scaling**:
    The same code that works on one machine also runs on a
    large cluster without any changes.
4. **Python first**:
    You can express your inference job directly in Python instead of
    YAML files or other formats.

.. _batch_inference_quickstart:

Quick Start
-----------

If you're impatient and want to see a copy-paste example right away,
here are a few simple examples.
Just pick one of the frameworks you like and run the code in your terminal.
If you want a more detailed rundown of the same examples, skip ahead to the
:ref:`following batch inference walk-through with Ray<batch_inference_walk_through>`.


.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_super_quick_start__
            :end-before: __hf_super_quick_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_super_quick_start__
            :end-before: __pt_super_quick_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_super_quick_start__
            :end-before: __tf_super_quick_end__


.. _batch_inference_walk_through:

Walk-through: Batch Inference with Ray
--------------------------------------

Running batch inference is conceptually easy and requires three steps:

1. Load your data and apply any preprocessing you need.
2. Define your model and define a transformation that applies your model to your data.
3. Run the transformation on your data.


Let's take a look at a simple example of this process without using Ray.
In each example we load ``batches`` of data, load a ``model``, define a ``transform``
function and apply the model to the data to get ``results``.

.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_no_ray_start__
            :end-before: __hf_no_ray_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_no_ray_start__
            :end-before: __pt_no_ray_end__

    .. group-tab:: TensorFlow

       .. literalinclude:: ./doc_code/tf_quick_start.py
           :language: python
           :start-after: __tf_no_ray_start__
           :end-before: __tf_no_ray_end__

.. note::

    As a Python user, this should all look familiar to you.
    The only part that you might be wondering about is that we're using
    ``Dict[str, np.ndarray]`` as input type to our ``transform`` functions.
    We do this to ease the transition to Ray, given that Ray Data uses
    ``Dict[str, np.ndarray]`` as the default format for its batches.


If you can follow the above examples conceptually, you should have no trouble scaling your batch
inference workload to a compute cluster with Ray Data.
If you're using Ray, the three steps for running batch inference read as follows:

1. Load a Ray Data dataset and apply any preprocessing you need. This will distribute your data
   across the cluster.
2. Define your model in a class and define a transformation that applies your model to
   your data batches (of format ``Dict[str, np.ndarray]`` by default).
3. Run inference on your data by using the :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
   method from Ray Data. In this step you also define how your batch processing job
   gets distributed across your cluster.

.. note::

    All advanced use cases ultimately boil down to extensions of the above three steps,
    like loading and storing data from cloud storage, using complex preprocessing functions,
    demanding model setups, additional postprocessing, or other customizations.
    We'll cover these advanced use cases in the next sections.

Let's scale out the above examples to a Ray cluster.
To start, install Ray with the data processing library, Ray Data:

.. code-block:: bash

    pip install ray[data]


1. Loading and preprocessing data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For this quick start guide we use very small, in-memory datasets by
leveraging common Python libraries like NumPy and Pandas.

In fact, we're using the exact same datasets as in the previous section, but load
them into Ray data.
The result of this step is a Dataset ``ds`` that we can use to run inference on.


.. tabs::

    .. group-tab:: HuggingFace

        Create a Pandas DataFrame with text data and convert it to a Dataset
        with the :meth:`ray.data.from_pandas() <ray.data.Dataset.from_pandas>` method.

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_quickstart_load_start__
            :end-before: __hf_quickstart_load_end__

    .. group-tab:: PyTorch

        Create a NumPy array with 100
        entries and convert it to a Dataset with the
        :meth:`ray.data.from_numpy() <ray.data.Dataset.from_numpy>` method.

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_quickstart_load_start__
            :end-before: __pt_quickstart_load_end__

    .. group-tab:: TensorFlow

        Create a NumPy array with 100
        entries and convert it to a Dataset with the
        :meth:`ray.data.from_numpy() <ray.data.Dataset.from_numpy>` method.

        .. literalinclude:: ./doc_code/tf_quick_start.py
           :language: python
           :start-after: __tf_quickstart_load_start__
           :end-before: __tf_quickstart_load_end__

2. Setting up your model
~~~~~~~~~~~~~~~~~~~~~~~~

Next, you want to set up your model for inference, by defining a predictor.
The core idea is to define a class that loads your model in its ``__init__`` method and
and implements a ``__call__`` method that takes a batch of data and returns a batch of predictions.
The ``__call__`` method is essentially the same as the ``transform`` function from the previous section.

Below you find examples for PyTorch, TensorFlow, and HuggingFace.

.. tabs::

    .. group-tab:: HuggingFace

        .. callout::

            .. literalinclude:: ./doc_code/hf_quick_start.py
                :language: python
                :start-after: __hf_quickstart_model_start__
                :end-before: __hf_quickstart_model_end__

            .. annotations::
                <1> Use the constructor (``__init__``) to initialize your model.

                <2> The ``__call__`` method runs inference on a batch of data.

    .. group-tab:: PyTorch

        .. callout::

            .. literalinclude:: ./doc_code/pytorch_quick_start.py
                :language: python
                :start-after: __pt_quickstart_model_start__
                :end-before: __pt_quickstart_model_end__

            .. annotations::
                <1> Use the constructor (``__init__``) to initialize your model.

                <2> The ``__call__`` method runs inference on a batch of data.


    .. group-tab:: TensorFlow

        .. callout::

            .. literalinclude:: ./doc_code/tf_quick_start.py
                :language: python
                :start-after: __tf_quickstart_model_start__
                :end-before: __tf_quickstart_model_end__

            .. annotations::
                <1> Use the constructor (``__init__``) to initialize your model.

                <2> The ``__call__`` method runs inference on a batch of data.


3. Getting predictions with Ray Data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Once you have your Dataset ``ds`` and your predictor class, you can use
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>` to get predictions.
``map_batches`` takes your predictor class as an argument and allows you to specify
``compute`` resources by defining the :class:`ActorPoolStrategy <ray.data.ActorPoolStrategy>`.
In the example below, we use two CPUs to run inference in parallel and then print the results.
We cover resource allocation in more detail in :ref:`the configuration section of this guide <batch_inference_config>`.

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


Note how defining your :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` function requires
you to write a Python method that takes a batch of data and returns a batch of predictions.
An easy way to do this and validate it is to use :meth:`ds.take_batch(N) <ray.data.Dataset.take_batch>` to get a batch of data
first, and then locally test your predictor method on that batch, without using Ray.
Once you are happy with the results, you can use the same function in ``map_batches``
on the full dataset. Below you see how to do that in our running examples.

.. tabs::

    .. group-tab:: HuggingFace

        .. literalinclude:: ./doc_code/hf_quick_start.py
            :language: python
            :start-after: __hf_quickstart_prediction_test_start__
            :end-before: __hf_quickstart_prediction_test_end__

    .. group-tab:: PyTorch

        .. literalinclude:: ./doc_code/pytorch_quick_start.py
            :language: python
            :start-after: __pt_quickstart_prediction_test_start__
            :end-before: __pt_quickstart_prediction_test_end__

    .. group-tab:: TensorFlow

        .. literalinclude:: ./doc_code/tf_quick_start.py
            :language: python
            :start-after: __tf_quickstart_prediction_test_start__
            :end-before: __tf_quickstart_prediction_test_end__


.. _batch_inference_config:

Configuration & Troubleshooting
-------------------------------

Configuring Batch Size
~~~~~~~~~~~~~~~~~~~~~~

An important parameter to set for :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
is ``batch_size``, which controls the size of the batches provided to the function.
Here's a simple example of loading the IRIS dataset (which has Pandas format by default)
and processing it with a batch size of `10`:

.. literalinclude:: ./doc_code/batch_formats.py
  :language: python
  :start-after: __simple_map_function_start__
  :end-before: __simple_map_function_end__

Increasing ``batch_size`` can result in faster execution by better leveraging vectorized
operations and hardware, reducing batch slicing and concatenation overhead, and overall
saturation of CPUs or GPUs.
On the other hand, this will also result in higher memory utilization, which can
lead to out-of-memory (OOM) failures.
If encountering OOMs, decreasing your ``batch_size`` may help.

.. caution::
  The default ``batch_size`` of ``4096`` may be too large for datasets with large rows
  (e.g. tables with many columns or a collection of large images).


.. _batch_inference_gpu:

Using GPUs in batch inference
-----------------------------

To use GPUs for inference, first pdate the callable class implementation to
move the model and data to and from Cuda device.
Here's a quick example for a PyTorch model:

.. code-block:: diff

    from torchvision.models import resnet18

    class TorchModel:
        def __init__(self):
            self.model = resnet18(pretrained=True)
    +       self.model = self.model.cuda()
            self.model.eval()

        def __call__(self, batch: Dict[str, np.ndarray]):
            torch_batch = torch.stack(batch["data"])
    +       torch_batch = torch_batch.cuda()
            with torch.inference_mode():
                prediction = self.model(torch_batch)
    -           return {"class": prediction.argmax(dim=1).detach().numpy()}
    +           return {"class": prediction.argmax(dim=1).detach().cpu().numpy()}


Next, specify ``num_gpus=N`` in :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
to indicate that each inference worker should use ``N`` GPUs.

.. code-block:: diff

    predictions = ds.map_batches(
        TorchModel,
        compute=ray.data.ActorPoolStrategy(size=2),
    +   num_gpus=1
    )

**How should I configure num_cpus and num_gpus for my model?**

By default, Ray will assign 1 CPU per task or actor. For example, on a machine
with 16 CPUs, this will result in 16 tasks or actors running concurrently for inference.
To change this, you can specify ``num_cpus=N``, which will tell Ray to reserve more CPUs
for the task or actor, or ``num_gpus=N``, which will tell Ray to reserve/assign GPUs
(GPUs will be assigned via `CUDA_VISIBLE_DEVICES` env var).

.. code-block:: python

    # Use 16 actors, each of which is assigned 1 GPU (16 GPUs total).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_gpus=1
    )

    # Use 16 actors, each of which is reserved 8 CPUs (128 CPUs total).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_cpus=8)


**How should I deal with OOM errors due to heavy model memory usage?**

It's common for models to consume a large amount of heap memory. For example, if a model
uses 5GB of RAM when created / run, and a machine has 16GB of RAM total, then no more
than three of these models can be run at the same time. The default resource assignments
of one CPU per task/actor will likely lead to OutOfMemoryErrors from Ray in this situation.

Let's suppose our machine has 16GiB of RAM and 8 GPUs. To tell Ray to construct at most
3 of these actors per node, we can override the CPU or memory:

.. code-block:: python

    # Require 5 CPUs per actor (so at most 3 can fit per 16 CPU node).
    ds = ds.map_batches(
        MyFn,
        compute=ActorPoolStrategy(size=16),
        num_cpus=5)

Learn more
----------


Batch inference is just one small part of the Machine Learning workflow, and only
a fraction of what Ray can do.

.. figure:: images/train_predict_pipeline.png

  How batch inference fits into the bigger picture of training and prediction AI models.

To learn more about Ray and batch inference, check out the following
tutorials and examples:

.. grid:: 1 2 3 4
    :gutter: 1
    :class-container: container pb-3

    .. grid-item-card::
        :img-top: /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /data/examples/nyc_taxi_basic_processing

            Batch Inference on NYC taxi data using Ray Data

    .. grid-item-card::
        :img-top: /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /data/examples/batch_inference_object_detection

            Object Detection Batch Inference with PyTorch

    .. grid-item-card::
        :img-top: /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /data/examples/ocr_example

            Batch OCR processing using Ray Data

    .. grid-item-card::
        :img-top:  /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /ray-air/examples/torch_detection

            Fine-tuning an Object Detection Model and using it for Batch Inference

    .. grid-item-card::
        :img-top: /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /ray-air/examples/torch_image_example

            Training an Image Classifier and using it for Batch Inference

    .. grid-item-card::
        :img-top: /images/ray_logo.png
        :class-img-top: pt-2 w-75 d-block mx-auto fixed-height-img

        .. button-ref:: /ray-air/examples/stablediffusion_batch_prediction

            Stable Diffusion Batch Prediction with Ray AIR
