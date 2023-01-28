---
jupyter:
  jupytext:
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.3'
      jupytext_version: 1.13.6
  kernelspec:
    display_name: .venv
    language: python
    name: python3
---

# Fine-tuning a Torch object detection model


This tutorial explains how to fine-tune an object detection model using Torch and the
[Ray AI Runtime](air) (AIR).

You should be familiar with [PyTorch](https://pytorch.org/) before starting the
tutorial. If you need a refresher, read PyTorch's
[training a classifier](https://pytorch.org/tutorials/beginner/blitz/cifar10_tutorial.html)
tutorial.


## Before you begin


* Install the [Ray AI Runtime](air).

```python
!pip install 'ray[air]'
```

* Install `torch`, `torchmetrics`, `torchvision`, and `xmltodict`.

```python
!pip install torch torchmetrics, torchvision xmltodict
```

## Create a `Dataset`


In this example, you'll train an object detection model on
[Pascal VOC](http://host.robots.ox.ac.uk/pascal/VOC/), a canonical object detection
dataset. The dataset contains 11,530 images across 20 different classes:

```python
CLASS_TO_LABEL = {
    "background": 0,
    "aeroplane": 1,
    "bicycle": 2,
    "bird": 3,
    "boat": 4,
    "bottle": 5,
    "bus": 6,
    "car": 7,
    "cat": 8,
    "chair": 9,
    "cow": 10,
    "diningtable": 11,
    "dog": 12,
    "horse": 13,
    "motorbike": 14,
    "person": 15,
    "pottedplant": 16,
    "sheep": 17,
    "sofa": 18,
    "train": 19,
    "tvmonitor": 20,
}
```

### Download Pascal VOC


First, download the 2GB of raw data:

```python
!curl -OJ http://host.robots.ox.ac.uk/pascal/VOC/voc2012/VOCtrainval_11-May-2012.tar
```

Then, untar the raw data to create the `VOCdevkit/VOC2012` folder:

```python
!tar -xvf VOCtrainval_11-May-2012.tar
```

```python
!ls VOCdevkit/VOC2012
```

### Define a custom datasource


Each image has an annotation:

```python
from PIL import Image

image = Image.open("VOCDevkit/VOC2012/JPEGImages/2007_000123.jpg")
display(image)

```

Each annotation describes the objects in an image:

```python
!cat VOCdevkit/VOC2012/Annotations/2007_000123.xml
```

A Datasource is an object that reads data of a particular type. For example, Ray
implements a Datasource that reads CSV files.

Ray doesn't provide built-in support for Pascal VOC annotations, so you'll need to define a
custom datasource. To implement the datasource, extend the built-in `FileBasedDatasource` class
and override the `_read_file` method.

```python
from typing import List, Tuple

import xmltodict
import pandas as pd
import pyarrow as pa

from ray.data.datasource import FileBasedDatasource
from ray.data.extensions import TensorArray


class VOCAnnotationDatasource(FileBasedDatasource):
    def _read_file(self, f: pa.NativeFile, path: str, **reader_args) -> pd.DataFrame:
        text = f.readall().decode("utf-8")
        annotation = xmltodict.parse(text)["annotation"]

        objects = annotation["object"]
        # If there's one object, `objects` is a `dict`; otherwise, it's a `list[dict]`.
        if isinstance(objects, dict):
            objects = [objects]

        boxes: List[Tuple] = []
        for obj in objects:
            x1 = float(obj["bndbox"]["xmin"])
            y1 = float(obj["bndbox"]["ymin"])
            x2 = float(obj["bndbox"]["xmax"])
            y2 = float(obj["bndbox"]["ymax"])
            boxes.append((x1, y1, x2, y2))

        labels: List[int] = [CLASS_TO_LABEL[obj["name"]] for obj in objects]

        filename = annotation["filename"]

        return pd.DataFrame(
            {
                "boxes": TensorArray([boxes]),
                "labels": TensorArray([labels]),
                "filename": [filename],
            }
        )

    def _rows_per_file(self):
        return 1
```

### Read annotations


To load the annotations into a `Dataset`, call `ray.data.read_datasource` and pass
your custom datasource to the constructor.

```python
import os
import ray


root = os.path.expanduser("~/Datasets/VOCdevkit/VOC2012")
annotations: ray.data.Dataset = ray.data.read_datasource(
    VOCAnnotationDatasource(), paths=os.path.join(root, "Annotations")
)
annotations.take(2)
```

### Load images into memory


Each row of `annotations` contains the filename of an image. Write a user-defined
function to open the images and add them to the dataset.

```python
from typing import Dict

import numpy as np
from PIL import Image


def read_images(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    images: List[np.ndarray] = []
    for filename in batch["filename"]:
        path = os.path.join(root, "JPEGImages", filename)
        image = np.array(Image.open(path))
        images.append(image)
    batch["image"] = np.array(images, dtype=object)
    return batch


dataset = annotations.map_batches(read_images)
dataset
```

### Split the dataset into train and test sets


Once you've created a `Dataset`, split the dataset into train and test sets.

```python
train_dataset, test_dataset = dataset.train_test_split(0.2)
```

## Define preprocessing logic


A `Preprocessor` is an object that defines preprocessing logic. It's the standard way
to preprocess data with Ray.

To preprocess the images, create a `TorchVisionPreprocessor` and call `transform` on the
dataset.


```python
from torchvision import transforms

from ray.data.preprocessors import TorchVisionPreprocessor

train_transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.RandomHorizontalFlip(p=0.5),
    ]
)
train_preprocessor = TorchVisionPreprocessor(
    columns=["image"], transform=train_transform
)
train_dataset = train_preprocessor.transform(train_dataset)

test_transform = transforms.ToTensor()
test_preprocessor = TorchVisionPreprocessor(columns=["image"], transform=test_transform)
test_dataset = test_preprocessor.transform(test_dataset)
```

## Fine-tune the object detection model


### Define the training loop


Write a function that trains `fasterrcnn_resnet50_fpn`. Your
function should contain standard Torch code with the following changes:
1. Wrap your model with `ray.train.torch.prepare_model` instead of `DistributedDataParallel`.
2. Distribute data with `session.get_dataset_shard` instead of `DistributedSampler`.
3. Iterate over data with `DatasetIterator.iter_batches` instead of `DataLoader`.
5. Report metrics and checkpoints with `session.report`.

```python
import torch
from torchvision import models

from ray.air import Checkpoint
from ray.air import session


def train_one_epoch(*, model, optimizer, batch_size, epoch):
    model.train()

    lr_scheduler = None
    if epoch == 0:
        warmup_factor = 1.0 / 1000
        lr_scheduler = torch.optim.lr_scheduler.LinearLR(
            optimizer, start_factor=warmup_factor, total_iters=250
        )

    device = ray.train.torch.get_device()
    train_dataset_shard = session.get_dataset_shard("train")

    batches = train_dataset_shard.iter_batches(batch_size=batch_size)
    for batch in batches:
        inputs = [torch.as_tensor(image).to(device) for image in batch["image"]]
        targets = [
            {
                "boxes": torch.as_tensor(boxes).to(device),
                "labels": torch.as_tensor(labels).to(device),
            }
            for boxes, labels in zip(batch["boxes"], batch["labels"])
        ]
        loss_dict = model(inputs, targets)
        losses = sum(loss for loss in loss_dict.values())

        optimizer.zero_grad()
        losses.backward()
        optimizer.step()

        if lr_scheduler is not None:
            lr_scheduler.step()

        session.report(
            {
                "losses": losses.item(),
                "epoch": epoch,
                "lr": optimizer.param_groups[0]["lr"],
                **{key: value.item() for key, value in loss_dict.items()},
            }
        )


def train_loop_per_worker(config):
    model = models.detection.fasterrcnn_resnet50_fpn(num_classes=21)
    model = ray.train.prepare_model(model)
    parameters = [p for p in model.parameters() if p.requires_grad]
    optimizer = torch.optim.SGD(
        parameters,
        lr=config["lr"],
        momentum=config["momentum"],
        weight_decay=config["weight_decay"],
    )
    lr_scheduler = torch.optim.lr_scheduler.MultiStepLR(
        optimizer, milestones=config["lr_steps"], gamma=config["lr_gamma"]
    )

    for epoch in range(0, config["epochs"]):
        train_one_epoch(
            model=model,
            optimizer=optimizer,
            batch_size=config["batch_size"],
            epoch=epoch,
        )
        lr_scheduler.step()
        checkpoint = Checkpoint.from_dict(
            {
                "model": model.module.state_dict(),
                "optimizer": optimizer.state_dict(),
                "lr_scheduler": lr_scheduler.state_dict(),
                "config": config,
                "epoch": epoch,
            }
        )
        session.report({}, checkpoint=checkpoint)
```

### Fine-tune the model


Once you've defined the training loop, create a `TorchTrainer` and pass the training
loop to the constructor. Then, call `TorchTrainer.fit` to train the model.

```python
from ray.air.config import ScalingConfig
from ray.train.torch import TorchTrainer


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "batch_size": 2,
        "lr": 0.02,
        "epochs": 26,
        "momentum": 0.9,
        "weight_decay": 1e-4,
        "lr_steps": [16, 22],
        "lr_gamma": 0.1,
    },
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    datasets={"train": train_dataset},
)
results = trainer.fit()
```

## Evaluate the model on test data


### Define a custom predictor


`Predictors` perform inference on batches of data.

To make `fasterrcnn_resnet50_fpn` outputs compatible with the `Predictor` interface,
subclass  `TorchPredictor` and override the `_predict_numpy` method.

```python
import collections

from ray.train.torch import TorchPredictor
from ray.air.util.tensor_extensions.pandas import _create_possibly_ragged_ndarray


class CustomTorchPredictor(TorchPredictor):
    def _predict_numpy(
        self, data: np.ndarray, dtype: torch.dtype
    ) -> Dict[str, np.ndarray]:
        inputs = [torch.as_tensor(image) for image in data["image"]]
        assert all(image.dim() == 3 for image in inputs)
        outputs = self.call_model(inputs)

        predictions = collections.defaultdict(list)
        for output in outputs:
            for key, value in output.items():
                predictions[key].append(value.cpu().detach().numpy())

        for key, value in predictions.items():
            predictions[key] = _create_possibly_ragged_ndarray(value)
        predictions = {"pred_" + key: value for key, value in predictions.items()}
        return predictions
```

### Generate predictions on the test data


Create a `BatchPredictor` and pass `CustomTorchPredictor` to the constructor. Then,
call `BatchPredictor.predict` to detect objects in the test dataset.

```python
from ray.train.batch_predictor import BatchPredictor


model = models.detection.fasterrcnn_resnet50_fpn(num_classes=21)
predictor = BatchPredictor(
    results.checkpoint, CustomTorchPredictor, model=model, use_gpu=True
)

predictions = predictor.predict(
    test_dataset,
    feature_columns=["image"],
    keep_columns=["boxes", "labels"],
    batch_size=4,
    max_scoring_workers=1,
)
predictions
```

### Evaluate the model


Once you've created the `predictions` dataset, iterate over the rows of the dataset
and compute the accuracy of the model.

```python
from torchmetrics.detection.mean_ap import MeanAveragePrecision


metric = MeanAveragePrecision()
for row in predictions.iter_rows():
    preds = [
        {
            "boxes": torch.as_tensor(row["pred_boxes"]),
            "scores": torch.as_tensor(row["pred_scores"]),
            "labels": torch.as_tensor(row["pred_labels"]),
        }
    ]
    target = [
        {
            "boxes": torch.as_tensor(row["boxes"]),
            "labels": torch.as_tensor(row["labels"]),
        }
    ]

metric.compute()
```
