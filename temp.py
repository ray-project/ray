from typing import Dict
import io
import numpy as np
import os
from PIL import Image
import ray


def show(dataset):
    Image.fromarray(dataset.take(1)[0]["image"]).show()


# dataset = ray.data.read_tfrecords(os.path.expanduser("~/Datasets/cifar-10/tfrecords"))


# def decode_bytes(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
#     images = []
#     for data in batch["image"]:
#         image = Image.open(io.BytesIO(data))
#         images.append(np.array(image))
#     batch["image"] = np.array(images)
#     return batch


# dataset = dataset.map_batches(decode_bytes, batch_format="numpy").fully_executed()

images = ray.data.read_numpy(os.path.expanduser("~/Datasets/cifar-10/images.npy"))
labels = ray.data.read_numpy(os.path.expanduser("~/Datasets/cifar-10/labels.npy"))
dataset = images.zip(labels)
dataset = dataset.map_batches(
    lambda batch: batch.rename(columns={"__value__": "image", "__value___1": "label"})
).fully_executed()

# dataset = ray.data.read_parquet(os.path.expanduser("~/Datasets/cifar-10/parquet"))
# print("read_parquet:", dataset)
# show(dataset)


# dataset = ray.data.read_images(
#     os.path.expanduser("~/Datasets/cifar-10/images"), include_paths=True
# )


# def add_class_column(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
#     classes = []
#     for path in batch["path"]:
#         # Paths look like '../tiny-imagenet/train/n01443537/n01443537_0.JPEG'
#         path = os.path.normpath(path)
#         parts = path.split(os.sep)
#         classes.append(parts[-2])
#     batch["class"] = np.array(classes)
#     return batch


# def remove_path_column(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
#     del batch["path"]
#     return batch


# dataset = (
#     dataset.map_batches(add_class_column)
#     .map_batches(remove_path_column)
#     .fully_executed()
# )

# show(dataset)
# print("read_images:", dataset)
# dataset = ray.data.read_tfrecords("s3://anonymous@air-example-data/cifar-10/images/")
# dataset = ray.data.read_tfrecords("s3://anonymous@air-example-data/cifar-10/data.tfrecords")
# images = ray.data.read_numpy("s3://anonymous@air-example-data/cifar-10/images.npy")
# labels = ray.data.read_numpy("s3://anonymous@air-example-data/cifar-10/labels.npy")
# dataset = ray.data.read_parquet("s3://anonymous@air-example-data/cifar-10/parquet")

# dataset = ray.data.read_images(
#     "s3://anonymous@air-example-data/cifar-10/", include_paths=True
# )


# def parse_path(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
#     classes = []
#     for path in batch["path"]:
#         # Paths look like '../tiny-imagenet/train/n01443537/n01443537_0.JPEG'
#         path = os.spath.normpath(path)
#         parts = path.split(os.sep)
#         classes.append(parts[-2])
#     batch["class"] = np.array(classes)
#     return batch


# dataset = dataset.map_batches(parse_path)

# print(dataset)

# from ray.train.torch import TorchTrainer

# from torchvision import models

# from ray.train.batch_predictor import BatchPredictor
# from ray.train.torch import TorchCheckpoint, TorchPredictor


# # preprocessor = None

# # model = models.resnet50()
# # checkpoint = TorchCheckpoint.from_model(model, preprocessor=preprocessor)
# # predictor = BatchPredictor.from_checkpoint(checkpoint, TorchCheckpoint)
# # predictor.predict(dataset)

from torchvision import models
from torchvision import transforms
from ray.data.preprocessors import TorchVisionPreprocessor

transform = transforms.Compose([transforms.ToTensor(), transforms.CenterCrop(224)])
preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

per_epoch_transform = transforms.RandomHorizontalFlip(p=0.5)
per_epoch_preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)


import torch.nn as nn
import torch.optim as optim

from ray import train
from ray.train.torch import TorchTrainer, TorchCheckpoint
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig


def train_one_epoch(model, *, criterion, optimizer, batch_size):
    dataset_shard = session.get_dataset_shard("train")
    for batch in dataset_shard.iter_torch_batches(batch_size=batch_size):
        inputs, labels = batch["image"], batch["label"]

        outputs = model(inputs)
        loss = criterion(outputs, labels)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def train_loop_per_worker(config):
    model = models.resnet50()
    model = train.torch.prepare_model(model)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(model.parameters(), lr=config["lr"])

    for epoch in range(config["epochs"]):
        train_one_epoch(
            model,
            criterion=criterion,
            optimizer=optimizer,
            batch_size=config["batch_size"],
        )

        metrics = {"epoch": epoch}
        checkpoint = TorchCheckpoint.from_state_dict(model.module.state_dict())
        session.report(metrics, checkpoint=checkpoint)


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"batch_size": 32, "lr": 0.02, "epochs": 90},
    datasets={"train": dataset},
    dataset_config={
        "train": DatasetConfig(per_epoch_preprocessor=per_epoch_preprocessor)
    },
    scaling_config=ScalingConfig(num_workers=2),
    preprocessor=preprocessor,
)
trainer.fit()
