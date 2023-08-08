.. _working_with_pytorch:

Working with PyTorch
====================

Ray Data integrates with the PyTorch ecosystem.

This guide describes how to:

* :ref:`Iterate over your dataset as torch tensors for model training <iterating_pytorch>`
* :ref:`Write transformations that deal with torch tensors <transform_pytorch>`
* :ref:`Perform batch inference with torch models <batch_inference_pytorch>`
* :ref:`Save Datasets containing torch tensors <saving_pytorch>`
* :ref:`Migrate from PyTorch Datasets to Ray Data <migrate_pytorch>`

.. _iterating_pytorch:

Iterating over torch tensors for training
-----------------------------------------
To iterate over batches of data in torch format, call :meth:`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`. Each batch is represented as `Dict[str, torch.Tensor]`, with one tensor per column in the dataset.

This is useful for training torch models with batches from your dataset. For configuration details such as providing a `collate_fn` for customizing the conversion, see `the API reference <ray.data.Dataset.iter_torch_batches>`.

.. testcode::

    import ray
    import torch

    ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

    for batch in ds.iter_torch_batches(batch_size=2):
        print(batch)

.. testoutput::
    :options: +MOCK

    {'image': tensor([[[[...]]]], dtype=torch.uint8)}
    ...
    {'image': tensor([[[[...]]]], dtype=torch.uint8)}

Integration with Ray Train
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Ray Data integrates with :ref:`Ray Train <train-docs>` for easy data ingest for data parallel training, with support for PyTorch, PyTorch Lightning, or Huggingface training.

.. testcode::

    import torch
    from torch import nn
    import ray
    from ray import train
    from ray.train import ScalingConfig
    from ray.train.torch import TorchTrainer

    def train_func(config):
        model = nn.Sequential(nn.Linear(30, 1), nn.Sigmoid())
        loss_fn = torch.nn.BCELoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=0.001)

        # Datasets can be accessed in your train_func via ``get_dataset_shard``.
        train_data_shard = train.get_dataset_shard("train")

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

For more details, see the :ref:`Ray Train user guide <data-ingest-torch>`.

.. _transform_pytorch:

Transformations with torch tensors
----------------------------------
Transformations applied with `map` or `map_batches` can return torch tensors.

.. caution::

    Under the hood, Ray Data automatically converts torch tensors to numpy arrays. Subsequent transformations accept numpy arrays as input, not torch tensors.

.. tab-set::

    .. tab-item:: map

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            def convert_to_torch(row: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
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

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            def convert_to_torch(batch: Dict[str, np.ndarray]) -> Dict[str, torch.Tensor]:
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

For more information on transforming data, see :ref:`Transforming data <transforming_data>`.

Built-in PyTorch transforms
~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use built-in torch transforms from `torchvision`, `torchtext`, and `torchaudio` Ray Data transformations.

.. tab-set::

    .. tab-item:: torchvision

        .. testcode::

            from typing import Dict
            import numpy as np
            import torch
            from torchvision import transforms
            import ray

            # Create the Dataset.
            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            # Define the torchvision transform.
            transform = transforms.Compose(
                [
                    transforms.ToTensor(),
                    transforms.CenterCrop(10)
                ]
            )

            # Define the map function
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
            ds = ray.data.read_text("s3://anonymous@ray-example-data/simple.txt")

            # Define the torchtext transform.
            VOCAB_FILE = "https://huggingface.co/bert-base-uncased/resolve/main/vocab.txt"
            transform = transforms.BERTTokenizer(vocab_path=VOCAB_FILE, do_lower_case=True, return_tokens=True)

            # Define the map_batches function.
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

With Ray Datasets, you can do scalable offline batch inference with torch models by mapping a pre-trained model over your data.

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

For more details, see the :ref:`Batch inference user guide <batch_inference_home>`.

.. _saving_pytorch:

Saving Datasets containing torch tensors
----------------------------------------

Datasets containing torch tensors can be saved to files, like parquet or numpy.

For more information on saving data, read
:ref:`Saving data <saving-data>`.

.. caution::

    Torch tensors that are on GPU devices can't be serialized and written to disk. Convert the tensors to CPU (``tensor.to("cpu")``) before saving the data.

.. tab-set::

    .. tab-item:: Parquet

        .. testcode::

            import torch
            import ray

            tensor = torch.Tensor(1)
            ds = ray.data.from_items([{"tensor": tensor}])

            ds.write_parquet("local:///tmp/tensor")

    .. tab-item:: Numpy

        .. testcode::

            import torch
            import ray

            tensor = torch.Tensor(1)
            ds = ray.data.from_items([{"tensor": tensor}])

            ds.write_numpy("local:///tmp/tensor", column="tensor")

.. _migrate_pytorch:

Migrating from PyTorch Datasets and DataLoaders
-----------------------------------------------

If you're currently using PyTorch Datasets and DataLoaders, you can migrate to Ray Data for working with distributed datasets.

PyTorch Datasets are replaced by the :class:`Dataset <ray.data.Dataset>` abtraction, and the PyTorch DataLoader is replaced by :meth:`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`.

Built-in PyTorch Datasets
~~~~~~~~~~~~~~~~~~~~~~~~~

If you are using built-in PyTorch datasets, for example from `torchvision`, these can be converted to a Ray Dataset using the :meth:`from_torch() <ray.data.from_torch>` API.

.. caution::

    :meth:`from_torch() <ray.data.from_torch>` requires the PyTorch Dataset to fit in memory. Use this only for small, built-in datasets for prototyping or testing.

.. testcode::

    import torchvision
    import ray

    mnist = torchvision.datasets.MNIST(root="/tmp/", download=True)
    ds = ray.data.from_torch(mnist)

    # The data for each item of the torch dataset is under the "item" key.
    print(ds.schema())

..
    The following `testoutput` is mocked to avoid illustrating download logs like
    "Downloading http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz".

.. testoutput::
    :options: +MOCK

    Column  Type
    ------  ----
    item    <class 'object'>

Custom PyTorch Datasets
~~~~~~~~~~~~~~~~~~~~~~~

If you have a custom PyTorch Dataset, you can migrate to Ray Data by converting the logic in ``__getitem__`` to Ray Data read and transform operations.

Any logic for reading data from cloud storage and disk can be replaced by one of the Ray Data ``read_*`` APIs, and any transformation logic can be applied as a :meth:`map <ray.data.Dataset.map>` call on the Dataset.

The following example shows a custom PyTorch Dataset, and what the analagous would look like with Ray Data.

.. note::

    Unlike PyTorch Map-style datasets, Ray Datasets are not indexable.

.. tab-set::

    .. tab-item:: PyTorch Dataset

        .. testcode::

            import tempfile
            import boto3
            from botocore import UNSIGNED
            from botocore.config import Config

            from torchvision import transforms
            from torch.utils.data import Dataset
            from PIL import Image

            class ImageDataset(Dataset):
                def __init__(self, bucket_name: str, dir_path: str):
                    self.s3 = boto3.resource("s3", config=Config(signature_version=UNSIGNED))
                    self.bucket = self.s3.Bucket(bucket_name)
                    self.files = [obj.key for obj in self.bucket.objects.filter(Prefix=dir_path)]

                    self.transform = transforms.Compose([
                        transforms.ToTensor(),
                        transforms.Resize((128, 128)),
                        transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
                    ])

                def __len__(self):
                    return len(self.files)

                def __getitem__(self, idx):
                    img_name = self.files[idx]

                    # Infer the label from the file name.
                    last_slash_idx = img_name.rfind("/")
                    dot_idx = img_name.rfind(".")
                    label = int(img_name[last_slash_idx+1:dot_idx])

                    # Download the S3 file locally.
                    obj = self.bucket.Object(img_name)
                    tmp = tempfile.NamedTemporaryFile()
                    tmp_name = "{}.jpg".format(tmp.name)

                    with open(tmp_name, "wb") as f:
                        obj.download_fileobj(f)
                        f.flush()
                        f.close()
                        image = Image.open(tmp_name)

                    # Preprocess the image.
                    image = self.transform(image)

                    return image, label

            dataset = ImageDataset(bucket_name="ray-example-data", dir_path="batoidea/JPEGImages/")

    .. tab-item:: Ray Data

        .. testcode::

            import torchvision
            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages", include_paths=True)

            # Extract the label from the file path.
            def extract_label(row: dict):
                filepath = row["path"]
                last_slash_idx = filepath.rfind("/")
                dot_idx = filepath.rfind('.')
                label = int(filepath[last_slash_idx+1:dot_idx])
                row["label"] = label
                return row

            transform = transforms.Compose([
                            transforms.ToTensor(),
                            transforms.Resize((128, 128)),
                            transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
                        ])

            # Preprocess the images.
            def transform_image(row: dict):
                row["transformed_image"] = transform(row["image"])
                return row

            # Map the transformations over the dataset.
            ds = ds.map(extract_label).map(transform_image)

PyTorch DataLoader
~~~~~~~~~~~~~~~~~~

The PyTorch DataLoader can be replaced by calling :meth:`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>` to iterate over batches of the dataset.

The following table describes how the arguments for PyTorch DataLoader map to Ray Data. Note the the behavior may not necessarily be identical. For exact semantics and usage, :meth:`see the API reference <ray.data.Dataset.iter_torch_batches>`.

.. list-table::
   :header-rows: 1

   * - PyTorch DataLoader arguments
     - Ray Data API
   * - ``batch_size``
     - ``batch_size`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
   * - ``shuffle``
     - ``local_shuffle_buffer_size`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
   * - ``collate_fn``
     - ``collate_fn`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
   * - ``sampler``
     - Not supported. Can be manually implemented after iterating through the dataset with :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`.
   * - ``batch_sampler``
     - Not supported. Can be manually implemented after iterating through the dataset with :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`.
   * - ``drop_last``
     - ``drop_last`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
   * - ``num_workers``
     - Use ``prefetch_batches`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>` to indicate how many batches to prefetch. The number of prefetching threads will automatically be configured according to ``prefetch_batches``.
   * - ``prefetch_factor``
     - Use ``prefetch_batches`` arg to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>` to indicate how many batches to prefetch. The number of prefetching threads will automatically be configured according to ``prefetch_batches``.
   * - ``pin_memory``
     - Pass in ``device`` to :meth:`ds.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>` to get tensors that have already been moved to the correct device.
