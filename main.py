import collections
import os
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import torch
import xmltodict
from PIL import Image
from torchmetrics.detection.mean_ap import MeanAveragePrecision
from torchvision import models, transforms

import ray
from ray import train
from ray.air import Checkpoint, session
from ray.air.config import ScalingConfig
from ray.air.util.tensor_extensions.pandas import _create_possibly_ragged_ndarray
from ray.data.block import Block
from ray.data.datasource import FileBasedDatasource
from ray.data.extensions import TensorArray
from ray.data.preprocessors import TorchVisionPreprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.torch import TorchPredictor, TorchTrainer


class VOCAnnotationDatasource(FileBasedDatasource):

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

    def _read_file(self, f: pa.NativeFile, path: str, **reader_args) -> Block:
        text = f.readall().decode("utf-8")
        xml = xmltodict.parse(text)
        assert set(xml) == {"annotation"}
        annotation = xml["annotation"]

        objects = annotation["object"]
        # If there's one object, `objects` is a `dict`; otherwise, it's a `list`.
        if isinstance(objects, dict):
            objects = [objects]

        boxes: List[Tuple] = []
        for obj in objects:
            x1 = float(obj["bndbox"]["xmin"])
            y1 = float(obj["bndbox"]["ymin"])
            x2 = float(obj["bndbox"]["xmax"])
            y2 = float(obj["bndbox"]["ymax"])
            boxes.append((x1, y1, x2, y2))

        labels: List[int] = [self.CLASS_TO_LABEL[obj["name"]] for obj in objects]

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


root = os.path.expanduser("~/Datasets/VOCdevkit/VOC2012")
annotations: ray.data.Dataset = ray.data.read_datasource(
    VOCAnnotationDatasource(), paths=os.path.join(root, "Annotations")
)


def read_images(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    images: list[np.ndarray] = []
    for filename in batch["filename"]:
        path = os.path.join(root, "JPEGImages", filename)
        image = np.array(Image.open(path))
        images.append(image)
    batch["image"] = np.array(images, dtype=object)
    return batch


dataset = annotations.map_batches(read_images)
train_dataset, test_dataset = dataset.train_test_split(0.2)

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


def train_one_epoch(*, model, optimizer, batch_size, epoch):
    model.train()

    lr_scheduler = None
    if epoch == 0:
        warmup_factor = 1.0 / 1000
        lr_scheduler = torch.optim.lr_scheduler.LinearLR(
            optimizer, start_factor=warmup_factor, total_iters=1000
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
    model = train.torch.prepare_model(model)
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
    start_epoch = 0

    checkpoint = session.get_checkpoint()
    if checkpoint is not None:
        checkpoint_data = checkpoint.to_dict()
        model.load_state_dict(checkpoint_data["model"])
        optimizer.load_state_dict(checkpoint_data["optimizer"])
        lr_scheduler.load_state_dict(checkpoint_data["lr_scheduler"])
        start_epoch = checkpoint_data["epoch"]

    for epoch in range(start_epoch, config["epochs"]):
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


trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "batch_size": 2,
        "lr": 0.02,
        # "epochs": 26,
        "epochs": 1,
        "momentum": 0.9,
        "weight_decay": 1e-4,
        "lr_steps": [16, 22],
        "lr_gamma": 0.1,
    },
    scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    # run_config=RunConfig(progress_reporter=CLIReporter)  TODO
    datasets={"train": train_dataset.limit(100)},
)
results = trainer.fit()


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


model = models.detection.fasterrcnn_resnet50_fpn(num_classes=21)

predictor = BatchPredictor(
    results.checkpoint, CustomTorchPredictor, model=model, use_gpu=False
)
predictions = predictor.predict(
    test_dataset.limit(100),
    feature_columns=["image"],
    keep_columns=["boxes", "labels"],
    batch_size=4,
    max_scoring_workers=1
)


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
    metric.update(preds, target)
print(metric.compute())
