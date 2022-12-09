import collections
import os
import warnings
from typing import Any, Dict, Literal, Callable
from functools import partial

import numpy as np
import ray
import torch
from torchvision import transforms
from pycocotools import mask as coco_mask
from pycocotools.coco import COCO
from ray import train
from ray.air import Checkpoint, session
from ray.air.config import ScalingConfig, RunConfig
from ray.air.util.tensor_extensions.pandas import _create_possibly_ragged_ndarray
from ray.train.batch_predictor import BatchPredictor
from ray.tune.progress_reporter import CLIReporter
from ray.train.torch import TorchPredictor, TorchTrainer
from torchvision import transforms, models
from ray.data import ActorPoolStrategy


def convert_path_to_filename(batch: Dict[str, Any]) -> Dict[str, Any]:
    batch["filename"] = np.array([os.path.basename(path) for path in batch["path"]])
    del batch["path"]
    return batch


class AddAnnotations:
    def __init__(self, root: str, split: Literal["train", "val"]):
        path = os.path.join(root, "annotations", f"instances_{split}2017.json")
        coco = COCO(path)

        self.filename_to_annotations = {}
        for image_id in coco.getImgIds():
            images: list[dict] = coco.loadImgs(image_id)
            assert len(images) == 1
            filename = images[0]["file_name"]

            annotation_ids = coco.getAnnIds(imgIds=image_id)
            if len(annotation_ids) == 0:
                continue

            annotations = coco.loadAnns(annotation_ids)
            assert len(annotations) > 0
            self.filename_to_annotations[filename] = annotations

    def __call__(self, batch):
        batch = self.filter_missing_annotations(batch)
        batch = self.add_boxes(batch)
        batch = self.add_labels(batch)
        batch = self.add_masks(batch)
        return batch

    def filter_missing_annotations(
        self, batch: Dict[str, np.ndarray]
    ) -> Dict[str, np.ndarray]:
        keep = [
            filename in self.filename_to_annotations for filename in batch["filename"]
        ]
        return {key: value[keep] for key, value in batch.items()}

    def add_boxes(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["boxes"] = []

        for image, filename in zip(batch["image"], batch["filename"]):
            annotations = self.filename_to_annotations[filename]
            boxes = [annotation["bbox"] for annotation in annotations]
            boxes = np.stack(boxes)

            # (X, Y, W, H) -> (X1, Y1, X2, Y2)
            boxes[:, 2:] += boxes[:, :2]

            height, width = image.shape[0:2]
            boxes[:, 0::2].clip(min=0, max=width)
            boxes[:, 1::2].clip(min=0, max=height)

            batch["boxes"].append(boxes)

        batch["boxes"] = np.array(batch["boxes"])
        return batch

    def add_labels(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["labels"] = []

        for filename in batch["filename"]:
            annotations = self.filename_to_annotations[filename]
            labels = np.array([annotation["category_id"] for annotation in annotations])
            batch["labels"].append(labels)

        batch["labels"] = np.array(batch["labels"])
        return batch

    def add_masks(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["masks"] = []

        for image, filename in zip(batch["image"], batch["filename"]):
            annotations = self.filename_to_annotations[filename]
            segmentations = [annotation["segmentation"] for annotation in annotations]
            height, width = image.shape[0:2]
            masks = convert_coco_poly_to_mask(segmentations, height, width)
            batch["masks"].append(masks)

        batch["masks"] = np.array(batch["masks"])

        return batch


def convert_coco_poly_to_mask(segmentations, height, width):
    masks = []
    for polygons in segmentations:
        rles = coco_mask.frPyObjects(polygons, height, width)
        mask = coco_mask.decode(rles)
        if len(mask.shape) < 3:
            mask = mask[..., None]
        mask = np.array(mask, dtype=np.uint8)
        mask = mask.any(axis=2)
        masks.append(mask)
    if masks:
        try:
            masks = np.stack(masks, axis=0)
        except ValueError as e:
            for mask in masks:
                if mask.shape != masks[0].shape:
                    print(mask.shape, masks[0].shape)
            masks = np.zeros((0, height, width), dtype=np.uint8)
    else:
        masks = np.zeros((0, height, width), dtype=np.uint8)
    return masks


def preprocess(
    batch: Dict[str, np.ndarray], transform: Callable[[np.ndarray], torch.Tensor]
) -> Dict[str, np.ndarray]:
    # TODO: Use `TorchvisionPreprocessor`
    batch["image"] = _create_possibly_ragged_ndarray(
        [transform(image).numpy() for image in batch["image"]]
    )
    return batch


def get_dataset(
    root: str,
    *,
    split: Literal["train", "val"],
    transform: Callable[[np.ndarray], torch.Tensor],
):
    ds = ray.data.read_images(
        os.path.join(root, f"{split}2017"), mode="RGB", include_paths=True
    )
    ds = ds.map_batches(convert_path_to_filename, batch_format="numpy")
    ds = ds.map_batches(
        AddAnnotations,
        fn_constructor_args=[root],
        fn_constructor_kwargs={"split": split},
        compute=ActorPoolStrategy(2, 8),
        batch_format="numpy",
    )
    ds = ds.map_batches(partial(preprocess, transform=transform), batch_format="numpy")
    return ds


root = "/mnt/cluster_storage/COCO/"

train_transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.RandomHorizontalFlip(p=0.5),
        transforms.ConvertImageDtype(torch.float),
    ]
)
train_dataset = get_dataset(root, split="train", transform=train_transform)

val_transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.ConvertImageDtype(torch.float),
    ]
)
val_dataset = get_dataset(root, split="val", transform=val_transform)


def train_one_epoch(*, model, optimizer, scaler, batch_size, epoch):
    model.train()

    lr_scheduler = None
    if epoch == 0:
        warmup_factor = 1.0 / 1000
        lr_scheduler = torch.optim.lr_scheduler.LinearLR(
            optimizer, start_factor=warmup_factor, total_iters=1000
        )

    device = ray.train.torch.get_device()
    train_dataset_shard = session.get_dataset_shard("train")

    num_samples = train_dataset_shard.count()
    num_samples_seen = 0

    batches = train_dataset_shard.iter_batches(batch_size=batch_size)
    for batch in batches:
        inputs = [torch.as_tensor(image).to(device) for image in batch["image"]]
        targets = [
            {
                "boxes": torch.as_tensor(boxes).to(device),
                "labels": torch.as_tensor(labels).to(device),
                "masks": torch.as_tensor(masks).to(device),
            }
            for boxes, labels, masks in zip(
                batch["boxes"], batch["labels"], batch["masks"]
            )
        ]
        with torch.cuda.amp.autocast(enabled=scaler is not None):
            loss_dict = model(inputs, targets)
            losses = sum(loss for loss in loss_dict.values())

        optimizer.zero_grad()
        if scaler is not None:
            scaler.scale(losses).backward()
            scaler.step(optimizer)
            scaler.update()
        else:
            losses.backward()
            optimizer.step()

        if lr_scheduler is not None:
            lr_scheduler.step()

        session.report(
            {
                "losses": losses.item(),
                "epoch": epoch,
                "progress": num_samples_seen / num_samples,
                "lr": optimizer.param_groups[0]["lr"],
                **{key: value.item() for key, value in loss_dict.items()},
            }
        )


def train_loop_per_worker(config):
    model = models.detection.maskrcnn_resnet50_fpn(num_classes=91)
    model = train.torch.prepare_model(model)
    parameters = [p for p in model.parameters() if p.requires_grad]
    optimizer = torch.optim.SGD(
        parameters,
        lr=config["lr"],
        momentum=config["momentum"],
        weight_decay=config["weight_decay"],
    )
    scaler = torch.cuda.amp.GradScaler() if config["amp"] else None
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
        scaler.load_state_dict(checkpoint_data["scaler"])
        start_epoch = checkpoint_data["epoch"]
        if config["amp"]:
            scaler.load_state_dict(checkpoint_data["scaler"])

    for epoch in range(start_epoch, config["epochs"]):
        train_one_epoch(
            model=model,
            optimizer=optimizer,
            scaler=scaler,
            batch_size=config["batch_size"],
            epoch=epoch,
        )
        lr_scheduler.step()
        checkpoint = Checkpoint.from_dict(
            {
                "model": model.module.state_dict(),
                "optimizer": optimizer.state_dict(),
                "lr_scheduler": lr_scheduler.state_dict(),
                "scaler": scaler.state_dict() if config["amp"] else None,
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
        "amp": True,
        "lr_steps": [16, 22],
        "lr_gamma": 0.1,
    },
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    # run_config=RunConfig(progress_reporter=CLIReporter)  TODO
    datasets={"train": train_dataset.limit(100)},
)
results = trainer.fit()


model = models.detection.maskrcnn_resnet50_fpn()


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


predictor = BatchPredictor(
    results.checkpoint, CustomTorchPredictor, model=model, use_gpu=True
)
predictions = predictor.predict(
    val_dataset,
    feature_columns=["image"],
    keep_columns=["boxes", "labels"],
    batch_size=4,
)


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
    metric.update(preds, target)
print(metric.compute())
