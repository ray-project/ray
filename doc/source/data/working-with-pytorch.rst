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
                features = torch.stack([batch[col_name] for col_name in batch.keys() if col_name != "target"], axis=1)
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

.. testoutput::
    :hide:

    ...
    
.. _transform_pytorch:

Transformations with torch tensors
----------------------------------
Transformations applied with `map` or `map_batches` can return PyTorch tensors. 
For more information on transforming data, read
:ref:`Transforming data <transforming_data>`.

.. caution::
    
    PyTorch tensors are automatically converted to Numpy arrays under the hood. Subsequent transformations accept Numpy arrays as input, not PyTorch tensors.

.. tab-set::

     .. tab-item:: map

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

    .. tab-item:: map_batches

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
                return batch
            
            transformed_ds.map_batches(check_numpy, batch_size=2).take_all()

        .. testoutput::
            
            Column  Type
            ------  ----
            tensor  numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

Built-in PyTorch transforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Built-in PyTorch transforms from `torchvision`, `torchtext`, and `torchaudio` can also be used in Ray Data transformations.

.. tab-set::

    .. tab-item:: torchvision

        .. testcode::
            
            from typing import Dict
            import numpy as np
            import torch
            from torchvision import transforms
            import ray
            
            # Create the Dataset.
            ds = ray.data.read_images("example://image-datasets/simple")

            # Define the torchvision transform.
            transform = transforms.Compose(
                [
                    transforms.ToTensor(),
                    transforms.CenterCrop(10)
                ]
            )

            # Define the map UDF
            def transform_image(row: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
                row["transformed_image"] = transform(row["image"])
                return row
            
            # Apply the transform over the dataset.
            transformed_ds = ds.map(transform_image)
            print(transformed_ds.schema())
        
        .. testoutput::

            Column             Type
            ------             ----
            image              numpy.ndarray(shape=(32, 32, 3), dtype=uint8)
            transformed_image  numpy.ndarray(shape=(3, 10, 10), dtype=float)
        
    .. tab-item:: torchtext

        .. testcode::

            from typing import Dict, List
            import numpy as np
            from torchtext import transforms
            import ray
            
            # Create the Dataset.
            ds = ray.data.read_text("example://simple.txt")

            # Define the torchtext transform.
            VOCAB_FILE = "https://huggingface.co/bert-base-uncased/resolve/main/vocab.txt"
            transform = transforms.BERTTokenizer(vocab_path=VOCAB_FILE, do_lower_case=True, return_tokens=True)

            # Define the map_batches UDF.
            def tokenize_text(batch: Dict[str, np.ndarray]) -> Dict[str, List[str]]:
                batch["tokenized_text"] = transform(list(batch["text"]))
                return batch
            
            # Apply the transform over the dataset.
            transformed_ds = ds.map_batches(tokenize_text, batch_size=2)
            print(transformed_ds.schema())
        
        .. testoutput::

            Column          Type
            ------          ----
            text            <class 'object'>
            tokenized_text  <class 'object'>

.. _batch_inference_pytorch:

Batch inference with PyTorch
----------------------------

With Ray Datasets, you can do scalable offline batch inference with PyTorch models by mapping your pre-trained model over your data. See the :ref:`Batch inference user guide <batch_inference_home>`` for more details and a full example.

.. _saving_pytorch:

Saving Datasets containing PyTorch Tensors
------------------------------------------

Datasets containing torch tensors can be saved to files, like parquet or numpy. For more information on saving data, read
:ref:`Saving data <saving-data>`.

.. tab-set::

     .. tab-item:: Parquet

        .. testcode::
            
            import torch
            import ray

            tensor = torch.Tensor(1)
            ds = ray.data.from_items([{"tensor": tensor}])

            ds.write_parquet("local:///tmp/tensor.parquet")

    .. tab-item:: Numpy

        .. testcode::
            
            import torch
            import ray

            tensor = torch.Tensor(1)
            ds = ray.data.from_items([{"tensor": tensor}])

            ds.write_numpy("local:///tmp/tensor.npy", column="tensor")

.. _migrate_pytorch:

Migrating from PyTorch Datasets and DataLoaders
-----------------------------------------------
