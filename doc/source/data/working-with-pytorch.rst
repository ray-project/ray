.. _working_with_pytorch:

Working with PyTorch
====================

Ray Data integrates with the PyTorch ecosystem.

This guide describes how to:
* `Iterate over your dataset as torch tensors for model training <#iterating_pytorch>`_
* `Write transformations that deal with torch tensors <#transform_pytorch>`_
* `Perform batch inference with torch models <#batch_inference_pytorch>`_
* `Save Datasets containing torch tensors <#save_pytorch>`_
* `Migrate from PyTorch Datasets to Ray Data <#migrate_pytorch>`_

.. _iterating_pytorch:

Iterating over torch tensors for training
-----------------------------------------
To iterate over batches of data in torch format, call :meth:`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`. Each batch is represented as `Dict[str, torch.Tensor]`, with one tensor per column in the dataset. 

This is useful for training torch models with batches from your dataset. See :ref:`the API reference <ray.data.Dataset.iter_torch_batches>` for more configuration details such as providing a `collate_fn` for customizing the conversion.

.. testcode::

    import ray
    import torch

    ds = ray.data.read_images("example://image-datasets/simple")

    for batch in ds.iter_torch_batches(batch_size=2):
        print(batch)

.. testoutput::
    :options: +MOCK

    {'image': tensor([[[[...]]]], dtype=torch.uint8)}
    ...
    {'image': tensor([[[[...]]]], dtype=torch.uint8)}

Integration with Ray Train
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Data also integrates with :ref:`Ray Train <train-docs>` for easy data ingest for data parallel training, with support for PyTorch, PyTorch Lightning, or Huggingface training.

See the :ref:`Ray Train user guide <train-datasets>` for more details.

.. testcode::
    
    import torch
    from torch import nn
    import ray
    from ray.air import session, ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        model = nn.Sequential(nn.Linear(30, 1), nn.Sigmoid())
        loss_fn = torch.nn.BCELoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=0.001)
        
        # Datasets can be accessed in your train_func via ``get_dataset_shard``.
        train_data_shard = session.get_dataset_shard("train")

        for epoch_idx in range(2):
            for batch in train_data_shard.iter_torch_batches(batch_size=128, dtypes=torch.float32):
                features = torch.cat([batch[col_name] for col_name in batch.keys() if col_name != "target"], axis=1)
                predictions = model(features)
                train_loss = loss_fn(predictions, batch["target"].unsqueeze(1))
                train_loss.backward()
                optimizer.step()

        
    train_dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")

    trainer = TorchTrainer(
        train_func,
        datasets={"train": train_dataset},
        scaling_config=ScalingConfig(num_workers=2)
    )
    trainer.fit()
    
.. _transform_pytorch:

Transformations with torch tensors
----------------------------------
Transformations applied with `map` or `map_batches` can return PyTorch tensors.

.. caution::
    
    PyTorch tensors are automatically converted to Numpy arrays under the hood. Subsequent transformations accept Numpy arrays as input, not PyTorch tensors.

.. tabs::

     .. group-tab:: map

        .. testcode::
            from typing import Dict
            import numpy as np
            import torch
            import ray
            

            ds = ray.data.read_images("example://image-datasets/simple")

            def convert_to_torch(row: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
                # Return torch tensor inside the UDF.
                return {"tensor": torch.as_tensor(row["image"])}
            
            # The tensor gets converted into a Numpy array under the hood
            transformed_ds = ds.map(convert_to_torch)
            print(transformed_ds.schema())

            # Subsequent transformations take in Numpy array as input.
            def check_numpy(row: Dict[str, np.ndarray]):
                assert isinstance(row["tensor"], np.ndarray)
                return row
            
            transformed_ds.map(check_numpy).take_all()

        .. testoutput::
            
            Column  Type
            ------  ----
            tensor  numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

    .. group-tab:: map_batches

        .. testcode::
            from typing import Dict
            import numpy as np
            import torch
            import ray
            

            ds = ray.data.read_images("example://image-datasets/simple")

            def convert_to_torch(batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
                # Return torch tensor inside the UDF.
                return {"tensor": torch.as_tensor(batch["image"])}
            
            # The tensor gets converted into a Numpy array under the hood
            transformed_ds = ds.map_batches(convert_to_torch, batch_size=2)
            print(transformed_ds.schema())

            # Subsequent transformations take in Numpy array as input.
            def check_numpy(batch: Dict[str, np.ndarray]):
                assert isinstance(batch["tensor"], np.ndarray)
                return row
            
            transformed_ds.map_batches(check_numpy, batch_size=2).take_all()

        .. testoutput::
            
            Column  Type
            ------  ----
            tensor  numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

Built-in PyTorch transforms from `torchvision`, `torchtext`, and `torchaudio` can also be used in Ray Data transformations.

.. code-snippet for torchvision and torchtext.

For more information on transforming data, read
:ref:`Transforming data <transforming_data>`.

.. _batch_inference_pytorch:

Batch inference with PyTorch
----------------------------

With Ray Datasets, you can do scalable offline batch inference with PyTorch models by mapping your pre-trained model over your data. See the :ref:`Batch inference user guide <batch_inference_home> for more details`.

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

.. _saving_pytorch:

Saving Datasets containing PyTorch Tensors
-------------------------------------------

.. _migrate_pytorch:

Migrating from PyTorch Datasets and DataLoaders
-----------------------------------------------