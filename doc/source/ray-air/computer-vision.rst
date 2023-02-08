.. _computer-vision:

Computer Vision
===============

Reading image datasets
----------------------

.. tabbed:: Images

    .. testcode::

        import ray

        dataset = ray.data.read_images("s3://anonymous@air-example-data/tiny-imagenet/", include_paths=True)

        print(dataset)

    .. testoutput::

        spam

    If your dataset encodes labels in paths, apply a user-defined function to parse
    labels.

    .. testcode::

        from typing import Dict

        def parse_paths(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            batch["label"] = np.array(parse(path) for path in batch["path"])
            return batch

        dataset = dataset.map_batches(parse_paths, batch_format="numpy")

        print(dataset)

    .. testoutput::

        spam

.. tabbed:: NumPy

    ...

.. tabbed:: TFRecords

    spam

.. tabbed:: Parquet

    spam

Transforming images
-------------------

.. tabbed:: Torch

    .. testcode::

        from torchvision import models

        from ray.data.preprocessors import TorchVisionPreprocessor

        model = models.resnet50()

        preprocessor = TorchVisionPreprocessor(transform=weights.transforms())
        per_epoch_preprocessor = TorchVisionPreprocessor(transform=weights.transforms())


.. tabbed:: TensorFlow

    .. testcode::

        from tensorflow.keras.applications import imagenet_utils

        from ray.data.preprocessors import BatchMapper

        def preprocess(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            batch["image"] = imagenet_utils.preprocess_input(batch["image"])
            return batch

        preprocessor = BatchMapper(preprocess, batch_format="numpy")


Training vision models
----------------------

.. tabbed:: Torch

    .. testcode::

        from ray.train.torch import TorchTrainer

        def train_loop_per_worker(config):
            model = models.resnet50()
            batches = session.get_dataset_shard("train")

        trainer = TorchTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config={"batch_size": 32, "lr": 0.02, "epochs": 90}
            datasets={"train": dataset},
            preprocessor=preprocessor
        )
        trainer.fit()

.. tabbed:: TensorFlow

    .. testcode::

        from ray.train.tensorflow import TensorflowTrainer

        def train_loop_per_worker(config):
            model.train()
            for batch_idx, (data, target) in enumerate(train_loader):
                data, target = data.to(device), target.to(device)
                optimizer.zero_grad()
                output = model(data)
                loss = F.nll_loss(output, target)
                loss.backward()
                optimizer.step()

        trainer = TensorflowTrainer(
            train_loop_per_worker=train_loop_per_worker,
            train_loop_config={"batch_size": 32, "lr": 0.02, "epochs": 90}
            datasets={"train": dataset},
            preprocessor=preprocessor
        )
        trainer.fit()


Batch image prediction
----------------------

.. tabbed:: Torch

    spam

.. tabbed:: TensorFlow

    ham

Online image prediction
-----------------------

.. tabbed:: Torch

    spam

.. tabbed:: TensorFlow

    ham
