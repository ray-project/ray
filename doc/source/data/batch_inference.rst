.. _batch_inference_home:

End-to-end: Offline Batch Inference
===================================

.. tip::

    `Get in touch with us <https://forms.gle/sGX7PQhheBGL6yxQ6>`_ if you are interested in Ray Data for offline batch inference. We believe we have the fastest & cheapest solution out there and would be happy to work with you on a POC.

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


.. _batch_inference_quickstart:

Quickstart
----------
To start, install Ray with the data processing library, Ray Data:

.. code-block:: bash

    pip install ray[data]

Then, use Ray Data for offline inference in 4 simple steps:

| **Step 1:** Load in your data into a Ray Dataset. Many different data types and formats are supported, see :ref:`Loading Data <loading_data>` for more details.
| **Step 2:** Loading the pre-trained model in a Python class. 
| **Step 3:** Transform your dataset using the pre-trained model by calling :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`. See :ref:`Transforming Data <transforming-data>` for more details.
| **Step 4:** Get the final predictions by either iterating through the output or saving the results. See :ref:`Consuming data <consuming_data>` for more details.

See :ref:`below <batch_inference_examples>` for more in-depth examples for your use case or how to :ref:`configure inference <batch_inference_configuration>`.

.. tabs::

    .. group-tab:: HuggingFace
        
        .. testcode::
            
            from typing import Dict
            import numpy as np

            import ray
            
            # Step 1: Create a Ray Dataset from in-memory Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

            # Step 2: Define a Predictor class for inference.
            # Use a class so the model can be initialized just once in `__init__`
            # and then re-used for inference across multiple batches.
            class HuggingFacePredictor:
                def __init__(self):
                    from transformers import pipeline
                    # Initialize a pre-trained GPT2 Huggingface pipeline.
                    self.model = pipeline("text-generation", model="gpt2")

                # Logic for inference on 1 batch of data.
                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, list]:
                    # Get the predictions from the input batch.
                    predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
                    # `predictions` is a list of length-one lists. For example:
                    # [[{'generated_text': 'output_1'}], ..., [{'generated_text': 'output_2'}]]
                    # Modify the output to get it into the following format instead:
                    # ['output_1', 'output_2']
                    batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
                    return batch

            # Use 2 parallel actors for inference. Each actor will predict on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the the predictor over the dataset to get predictions.
            predictions = ds.map_batches(HuggingFacePredictor, compute=scale)
            # Step 4: Show one prediction output.
            predictions.show(limit=1)
        
        .. testoutput::

            {'data': 'Complete this', 'output': "Complete this article through the web and check our FAQ.\n\nAre you a vegetarian? We'll"}
        

    .. group-tab:: PyTorch

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            import torch.nn as nn

            import ray

            # Step 1: Create a Ray Dataset from in-memory Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.ones((1, 100)))

            # Step 2: Define a Predictor class for inference.
            # Use a class so the model can be initialized just once in `__init__`
            # and then re-used for inference across multiple batches.
            class TorchPredictor:
                def __init__(self):
                    # Load a dummy neural network.
                    # Replace this with your pre-trained model.
                    self.model = nn.Sequential(
                        nn.Linear(in_features=100, out_features=1),
                        nn.Sigmoid(),
                    )
                    self.model.eval()

                # Logic for inference on 1 batch of data.
                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    tensor = torch.as_tensor(batch["data"], dtype=torch.float32)
                    with torch.inference_mode():
                        # Get the predictions from the input batch.
                        return {"output": self.model(tensor).numpy()}

            # Use 2 parallel actors for inference. Each actor will predict on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the the predictor over the dataset to get predictions.
            predictions = ds.map_batches(TorchPredictor, compute=scale)
            # Step 4: Show one prediction output.
            predictions.show(limit=1)

        .. testoutput::

            {'output': array([0.5590901], dtype=float32)}

    .. group-tab:: TensorFlow

        .. testcode::

            from typing import Dict
            import numpy as np

            import ray
            
            # Step 1: Create a Ray Dataset from in-memory Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.ones((1, 100)))

            # Step 2: Define a Predictor class for inference.
            # Use a class so the model can be initialized just once in `__init__`
            # and then re-used for inference across multiple batches.
            class TFPredictor:
                def __init__(self):
                    from tensorflow import keras

                    # Load a dummy neural network.
                    # Replace this with your pre-trained model.
                    input_layer = keras.Input(shape=(100,))
                    output_layer = keras.layers.Dense(1, activation="sigmoid")
                    self.model = keras.Sequential([input_layer, output_layer])

                # Logic for inference on 1 batch of data.
                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    # Get the predictions from the input batch.
                    return {"output": self.model(batch["data"]).numpy()}

            # Use 2 parallel actors for inference. Each actor will predict on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the the predictor over the dataset to get predictions.
            predictions = ds.map_batches(TFPredictor, compute=scale)
             # Step 4: Show one prediction output.
            predictions.show(limit=1)

        .. testoutput::

            {'output': array([0.625576], dtype=float32)}

.. _batch_inference_examples:

More Examples
-------------
- :doc:`Image Classification Batch Inference with PyTorch ResNet18 </data/examples/pytorch_resnet_batch_prediction>` 
- :doc:`Object Detection Batch Inference with PyTorch FasterRCNN_ResNet50 </data/examples/batch_inference_object_detection>`
- :doc:`Image Classification Batch Inference with Huggingface Vision Transformer </data/examples/huggingface_vit_batch_prediction>`

.. _batch_inference_configuration:

Configuration & Troubleshooting
-------------------------------

.. _batch_inference_gpu:

Using GPUs for inference
~~~~~~~~~~~~~~~~~~~~~~~~

To use GPUs for inference, make the following changes to your code:

1. Update the class implementation to move the model and data to and from GPU device.
2. Specify `num_gpus=1` in the :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` or call to indicate that each actor should use 1 GPU. 

The rest of the code can remain the same as the :ref:`Quickstart <batch_inference_quickstart>`.

.. tabs::

    .. group-tab:: HuggingFace
        
        .. testcode::
            
            from typing import Dict
            import numpy as np

            import ray
            
            ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

            class HuggingFacePredictor:
                def __init__(self):
                    from transformers import pipeline
                    # Set "cuda:0" as the device so the Huggingface pipeline uses GPU.
                    self.model = pipeline("text-generation", model="gpt2", device="cuda:0")

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, list]:
                    predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
                    batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
                    return batch

            # Use 2 actors, each actor using 1 GPU. 2 GPUs total.
            predictions = ds.map_batches(
                HuggingFacePredictor, 
                num_gpus=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2) 
                )
            predictions.show(limit=1)
        
        .. testoutput::

            {'data': 'Complete this', 'output': "Complete this article through the web and check our FAQ.\n\nAre you a vegetarian? We'll"}
        

    .. group-tab:: PyTorch

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            import torch.nn as nn

            import ray

            ds = ray.data.from_numpy(np.ones((1, 100)))

            class TorchPredictor:
                def __init__(self):
                    # Move the neural network to GPU device by specifying "cuda".
                    self.model = nn.Sequential(
                        nn.Linear(in_features=100, out_features=1),
                        nn.Sigmoid(),
                    ).cuda()
                    self.model.eval()

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    # Move the input batch to GPU device by specifying "cuda".
                    tensor = torch.as_tensor(batch["data"], dtype=torch.float32, device="cuda")
                    with torch.inference_mode():
                        # Move the prediction output back to CPU before returning.
                        return {"output": self.model(tensor).cpu().numpy()}

            # Use 2 actors, each actor using 1 GPU. 2 GPUs total.
            predictions = ds.map_batches(
                TorchPredictor, 
                num_gpus=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2) 
                )
            predictions.show(limit=1)

        .. testoutput::

            {'output': array([0.5590901], dtype=float32)}

    .. group-tab:: TensorFlow

        .. testcode::

            from typing import Dict
            import numpy as np

            import ray
            
            ds = ray.data.from_numpy(np.ones((1, 100)))

            class TFPredictor:
                def __init__(self):
                    import tensorflow as tf
                    from tensorflow import keras
                    
                    # Move the neural network to GPU by specifying the GPU device.
                    with tf.device("GPU:0"):
                        input_layer = keras.Input(shape=(100,))
                        output_layer = keras.layers.Dense(1, activation="sigmoid")
                        self.model = keras.Sequential([input_layer, output_layer])

                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    # Move the input batch to GPU by specifying GPU device.
                    with tf.device("GPU:0"):
                        return {"output": self.model(batch["data"]).numpy()}

            # Use 2 actors, each actor using 1 GPU. 2 GPUs total.
            predictions = ds.map_batches(
                TFPredictor, 
                num_gpus=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2) 
                )
            predictions.show(limit=1)

        .. testoutput::

            {'output': array([0.625576], dtype=float32)}

.. _batch_inference_batch_size:

Configuring Batch Size
~~~~~~~~~~~~~~~~~~~~~~

You can configure the size of the input batch that is passed to ``__call__`` by setting the ``batch_size`` argument for :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`

Increasing batch size results in faster execution since inference is a vectorized operation. For GPU inference, increasing batch size increases GPU utilization. Generally, you want to set the batch size to as large possible without running out of memory. If encountering OOMs, decreasing your ``batch_size`` may help.

.. testcode::
    
    import numpy as np

    import ray

    ds = ray.data.from_numpy(np.ones((10, 100)))

    def assert_batch(batch: Dict[str, np.ndarray]):
        assert len(batch) == 2
        return batch
    
    # Specify that each input batch should be of size 2.
    ds.map_batches(assert_batch, batch_size=2)

.. caution::
  The default ``batch_size`` of ``4096`` may be too large for datasets with large rows
  (e.g. tables with many columns or a collection of large images).

Handling GPU out-of-memory failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are running into CUDA out-of-memory issues, it is likely that your batch size is too large. Decrease the batch size by following :ref:`these steps <_batch_inference_batch_size>`.

If your batch size is already set to 1, then either use a smaller model or use GPU devices with more memory.

For advanced users working with large models, you can use model parallelism to shard your model across multiple GPUs.

Optimizing expensive CPU preprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your workload involves expensive CPU preprocessing in addition to model inference, you can optimize throughput by separating the preprocessing and inference logic into separate stages. This allows inference on batch N to execute concurrently with preprocessing on batch N+1.

See :doc:`Image Classification Batch Inference with PyTorch ResNet18 </data/examples/pytorch_resnet_batch_prediction>` for an example where preprocessing is done in a separate `map` call than inference.

Handling CPU out-of-memory failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you are running out of CPU RAM, it is likely that you have too many model replicas that are being run concurrently on the same node. For example, if a model
uses 5GB of RAM when created / run, and a machine has 16GB of RAM total, then no more
than three of these models can be run at the same time. The default resource assignments
of one CPU per task/actor will likely lead to OutOfMemoryErrors from Ray in this situation.

Let's suppose our cluster has 4 nodes, each with 16 CPUs. To limit to at most
3 of these actors per node, we can override the CPU or memory:

.. testcode::
    :skipif: True
            
    from typing import Dict
    import numpy as np

    import ray
    
    ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

    class HuggingFacePredictor:
        def __init__(self):
            from transformers import pipeline
            self.model = pipeline("text-generation", model="gpt2")

        def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, list]:
            predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
            batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
            return batch

    predictions = ds.map_batches(
        HuggingFacePredictor, 
        # Require 5 CPUs per actor (so at most 3 can fit per 16 CPU node).
        num_cpus=5,
        # 3 actors per node, with 4 nodes in the cluster means ActorPool size of 12.
        compute=ray.data.ActorPoolStrategy(size=12) 
        )
    predictions.show(limit=1)

How does Ray Data compare to X for offline inference?
-----------------------------------------------------

.. dropdown:: Batch Services: AWS Batch, GCP Batch

    Cloud providers such as AWS, GCP, and Azure provide batch services to manage compute infrastructure for you. Regardless of which service you choose, the process is the same: you provide your code, and the service runs your code on each node in a cluster. However, while infrastructure management is necessary, it is not enough. These services have limitations, such as a lack of software libraries to address optimized parallelization, efficient data transfer, and easy debugging. These solutions are suitable only for experienced users who can write their own optimized batch inference code.

    Ray Data abstracts away not only the infrastructure management, but also sharding your dataset, parallelizing the inference over these shards, and transferring data from storage to CPU to GPU.


.. dropdown:: Online Inference Solutions: Bento ML, Sagemaker Batch Transform

    Solutions like `Bento ML <https://www.bentoml.com/>`_, `Sagemaker Batch Transform <https://docs.aws.amazon.com/sagemaker/latest/dg/batch-transform.html>`_, or :ref:`Ray Serve <rayserve>` provide APIs to make it easy to write performant inference code and can abstract away infrastructure complexities. But they are designed for online inference rather than offline batch inference, which are two different problems with different sets of requirements. These solutions often don't perform well in the offline case, leading inference service providers like `Bento ML to integrating with Apache Spark <https://modelserving.com/blog/unifying-real-time-and-batch-inference-with-bentoml-and-spark>`_ for offline inference.

    Ray Data is built for offline batch jobs, without all the extra complexities of starting servers or sending HTTP requests.

    See `our blog <https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker>`_ for a more detailed performance comparison between Ray Data and Sagemaker Batch Transform.

.. dropdown:: Distributed Data Processing Frameworks: Apache Spark

    Ray Data handles many of the same batch processing workloads as `Apache Spark <https://spark.apache.org/>`_, but with its streaming paradigm, it is better suited for GPU workloads that arise in deep learning inference.

    See `our blog <https://www.anyscale.com/blog/offline-batch-inference-comparing-ray-apache-spark-and-sagemaker>`_ for a more detailed performance comparison between Ray Data and Apache Spark.

Case studies
------------
- `Sewer AI speeds up object detection on videos 3x using Ray Data <https://www.anyscale.com/blog/inspecting-sewer-line-safety-using-thousands-of-hours-of-video>`_