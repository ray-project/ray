.. _batch_inference_home:

End-to-end: Offline Batch Inference
===================================

.. tip::

    `Get in touch <https://forms.gle/sGX7PQhheBGL6yxQ6>`_ to get help using Ray Data, the industry's fastest and cheapest solution for offline batch inference.

Offline batch inference is a process for generating model predictions on a fixed set of input data. Ray Data offers an efficient and scalable solution for batch inference, providing faster execution and cost-effectiveness for deep learning applications.

For an overview on why you should use Ray Data for offline batch inference, and how it compares to alternatives, see the :ref:`Ray Data Overview <data_overview>`.

.. figure:: images/batch_inference.png


.. _batch_inference_quickstart:

Quickstart
----------
To start, install Ray Data:

.. code-block:: bash

    pip install -U "ray[data]"

Using Ray Data for offline inference involves four basic steps:

- **Step 1:** Load your data into a Ray Dataset. Ray Data supports many different datasources and formats. For more details, see :ref:`Loading Data <loading_data>`.
- **Step 2:** Define a Python class to load the pre-trained model.
- **Step 3:** Transform your dataset using the pre-trained model by calling :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`. For more details, see :ref:`Transforming Data <transforming_data>`.
- **Step 4:** Get the final predictions by either iterating through the output or saving the results. For more details, see the :ref:`Iterating over data <iterating-over-data>` and :ref:`Saving data <saving-data>` user guides.

For more in-depth examples for your use case, see :ref:`the batch inference examples<batch_inference_examples>`.
For how to configure batch inference, see :ref:`the configuration guide<batch_inference_configuration>`.

.. tab-set::

    .. tab-item:: HuggingFace
        :sync: HuggingFace

        .. testcode::

            from typing import Dict
            import numpy as np

            import ray

            # Step 1: Create a Ray Dataset from in-memory Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

            # Step 2: Define a Predictor class for inference.
            # Use a class to initialize the model just once in `__init__`
            # and re-use it for inference across multiple batches.
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

            # Use 2 parallel actors for inference. Each actor predicts on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the Predictor over the Dataset to get predictions.
            predictions = ds.map_batches(HuggingFacePredictor, compute=scale)
            # Step 4: Show one prediction output.
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'data': 'Complete this', 'output': 'Complete this information or purchase any item from this site.\n\nAll purchases are final and non-'}


    .. tab-item:: PyTorch
        :sync: PyTorch

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
            # Use a class to initialize the model just once in `__init__`
            # and re-use it for inference across multiple batches.
            class TorchPredictor:
                def __init__(self):
                    # Load a dummy neural network.
                    # Set `self.model` to your pre-trained PyTorch model.
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

            # Use 2 parallel actors for inference. Each actor predicts on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the Predictor over the Dataset to get predictions.
            predictions = ds.map_batches(TorchPredictor, compute=scale)
            # Step 4: Show one prediction output.
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'output': array([0.5590901], dtype=float32)}

    .. tab-item:: TensorFlow
        :sync: TensorFlow

        .. testcode::

            from typing import Dict
            import numpy as np

            import ray

            # Step 1: Create a Ray Dataset from in-memory Numpy arrays.
            # You can also create a Ray Dataset from many other sources and file
            # formats.
            ds = ray.data.from_numpy(np.ones((1, 100)))

            # Step 2: Define a Predictor class for inference.
            # Use a class to initialize the model just once in `__init__`
            # and re-use it for inference across multiple batches.
            class TFPredictor:
                def __init__(self):
                    from tensorflow import keras

                    # Load a dummy neural network.
                    # Set `self.model` to your pre-trained Keras model.
                    input_layer = keras.Input(shape=(100,))
                    output_layer = keras.layers.Dense(1, activation="sigmoid")
                    self.model = keras.Sequential([input_layer, output_layer])

                # Logic for inference on 1 batch of data.
                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
                    # Get the predictions from the input batch.
                    return {"output": self.model(batch["data"]).numpy()}

            # Use 2 parallel actors for inference. Each actor predicts on a
            # different partition of data.
            scale = ray.data.ActorPoolStrategy(size=2)
            # Step 3: Map the Predictor over the Dataset to get predictions.
            predictions = ds.map_batches(TFPredictor, compute=scale)
             # Step 4: Show one prediction output.
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'output': array([0.625576], dtype=float32)}

.. _batch_inference_examples:

More examples
-------------
- :doc:`Image Classification Batch Inference with PyTorch ResNet18 </data/examples/pytorch_resnet_batch_prediction>`
- :doc:`Object Detection Batch Inference with PyTorch FasterRCNN_ResNet50 </data/examples/batch_inference_object_detection>`
- :doc:`Image Classification Batch Inference with Hugging Face Vision Transformer </data/examples/huggingface_vit_batch_prediction>`

.. _batch_inference_configuration:

Configuration and troubleshooting
---------------------------------

.. _batch_inference_gpu:

Using GPUs for inference
~~~~~~~~~~~~~~~~~~~~~~~~

To use GPUs for inference, make the following changes to your code:

1. Update the class implementation to move the model and data to and from GPU.
2. Specify ``num_gpus=1`` in the :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` call to indicate that each actor should use 1 GPU.
3. Specify a ``batch_size`` for inference. For more details on how to configure the batch size, see :ref:`Configuring Batch Size <batch_inference_batch_size>`.

The remaining is the same as the :ref:`Quickstart <batch_inference_quickstart>`.

.. tab-set::

    .. tab-item:: HuggingFace
        :sync: HuggingFace

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
                # Specify the batch size for inference.
                # Increase this for larger datasets.
                batch_size=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2),
                )
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'data': 'Complete this', 'output': 'Complete this poll. Which one do you think holds the most promise for you?\n\nThank you'}


    .. tab-item:: PyTorch
        :sync: PyTorch

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
                # Specify the batch size for inference.
                # Increase this for larger datasets.
                batch_size=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2)
                )
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'output': array([0.5590901], dtype=float32)}

    .. tab-item:: TensorFlow
        :sync: TensorFlow

        .. testcode::

            from typing import Dict
            import numpy as np
            import tensorflow as tf
            from tensorflow import keras

            import ray

            ds = ray.data.from_numpy(np.ones((1, 100)))

            class TFPredictor:
                def __init__(self):
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
                # Specify the batch size for inference.
                # Increase this for larger datasets.
                batch_size=1,
                # Set the ActorPool size to the number of GPUs in your cluster.
                compute=ray.data.ActorPoolStrategy(size=2)
                )
            predictions.show(limit=1)

        .. testoutput::
            :options: +MOCK

            {'output': array([0.625576], dtype=float32)}

.. _batch_inference_batch_size:

Configuring Batch Size
~~~~~~~~~~~~~~~~~~~~~~

Configure the size of the input batch that's passed to ``__call__`` by setting the ``batch_size`` argument for :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`

Increasing batch size results in faster execution because inference is a vectorized operation. For GPU inference, increasing batch size increases GPU utilization. Set the batch size to as large possible without running out of memory. If you encounter out-of-memory errors, decreasing ``batch_size`` may help.

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
  (for example, tables with many columns or a collection of large images).

Handling GPU out-of-memory failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you run into CUDA out-of-memory issues, your batch size is likely too large. Decrease the batch size by following :ref:`these steps <batch_inference_batch_size>`.

If your batch size is already set to 1, then use either a smaller model or GPU devices with more memory.

For advanced users working with large models, you can use model parallelism to shard the model across multiple GPUs.

Optimizing expensive CPU preprocessing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your workload involves expensive CPU preprocessing in addition to model inference, you can optimize throughput by separating the preprocessing and inference logic into separate stages. This separation allows inference on batch :math:`N` to execute concurrently with preprocessing on batch :math:`N+1`.

For an example where preprocessing is done in a separate `map` call, see :doc:`Image Classification Batch Inference with PyTorch ResNet18 </data/examples/pytorch_resnet_batch_prediction>`.

Handling CPU out-of-memory failures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you run out of CPU RAM, you likely that you have too many model replicas that are running concurrently on the same node. For example, if a model
uses 5 GB of RAM when created / run, and a machine has 16 GB of RAM total, then no more
than three of these models can be run at the same time. The default resource assignments
of one CPU per task/actor might lead to `OutOfMemoryError` from Ray in this situation.

Suppose your cluster has 4 nodes, each with 16 CPUs. To limit to at most
3 of these actors per node, you can override the CPU or memory:

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


.. _batch_inference_ray_train:


Using models from Ray Train
---------------------------

Models that have been trained with :ref:`Ray Train <train-docs>` can then be used for batch inference with :ref:`Ray Data <data>` via the :class:`Checkpoint <ray.train.Checkpoint>` that is returned by :ref:`Ray Train <train-docs>`.

**Step 1:** Train a model with :ref:`Ray Train <train-docs>`.

.. testcode::

    import ray
    from ray.train import ScalingConfig
    from ray.train.xgboost import XGBoostTrainer

    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
    train_dataset, valid_dataset = dataset.train_test_split(test_size=0.3)

    trainer = XGBoostTrainer(
        scaling_config=ScalingConfig(
            num_workers=2,
            use_gpu=False,
        ),
        label_column="target",
        num_boost_round=20,
        params={
            "objective": "binary:logistic",
            "eval_metric": ["logloss", "error"],
        },
        datasets={"train": train_dataset, "valid": valid_dataset},
    )
    result = trainer.fit()


**Step 2:** Extract the :class:`Checkpoint <ray.train.Checkpoint>` from the training :class:`Result <ray.train.Result>`.

.. testcode::

    checkpoint = result.checkpoint

**Step 3:** Use Ray Data for batch inference. To load in the model from the :class:`Checkpoint <ray.train.Checkpoint>` inside the Python class, use the methodology corresponding to the Trainer used to train the model.

- **Deep Learning Trainers:** :ref:`train-checkpointing`
- **Tree-Based Trainers:** :ref:`train-gbdt-checkpoints`

In this case, use :meth:`XGBoostTrainer.get_model() <ray.train.xgboost.XGBoostTrainer.get_model>` to load the model.

The rest of the logic looks the same as in the `Quickstart <#quickstart>`_.

.. testcode::

    from typing import Dict
    import pandas as pd
    import numpy as np
    import xgboost

    from ray.train import Checkpoint
    from ray.train.xgboost import XGBoostTrainer

    test_dataset = valid_dataset.drop_columns(["target"])

    class XGBoostPredictor:
        def __init__(self, checkpoint: Checkpoint):
            self.model = XGBoostTrainer.get_model(checkpoint)

        def __call__(self, data: pd.DataFrame) -> Dict[str, np.ndarray]:
            dmatrix = xgboost.DMatrix(data)
            return {"predictions": self.model.predict(dmatrix)}


    # Use 2 parallel actors for inference. Each actor predicts on a
    # different partition of data.
    scale = ray.data.ActorPoolStrategy(size=2)
    # Map the Predictor over the Dataset to get predictions.
    predictions = test_dataset.map_batches(
        XGBoostPredictor,
        compute=scale,
        batch_format="pandas",
        # Pass in the Checkpoint to the XGBoostPredictor constructor.
        fn_constructor_kwargs={"checkpoint": checkpoint}
    )
    predictions.show(limit=1)

.. testoutput::
    :options: +MOCK

    {'predictions': 0.9969483017921448}
