# This file is adapted from https://github.com/Eventual-Inc/Daft/tree/9da265d8f1e5d5814ae871bed3cee1b0757285f5/benchmarking/ai/video_object_detection
from __future__ import annotations

import torch
import torchvision
from PIL import Image
from ultralytics import YOLO
import uuid
import daft
from daft.expressions import col

NUM_GPU_NODES = 8
YOLO_MODEL = "yolo11n.pt"
INPUT_PATH = "s3://anonymous@ray-example-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640


@daft.udf(
    return_dtype=daft.DataType.list(
        daft.DataType.struct(
            {
                "label": daft.DataType.string(),
                "confidence": daft.DataType.float32(),
                "bbox": daft.DataType.list(daft.DataType.int32()),
            }
        )
    ),
    concurrency=NUM_GPU_NODES,
    num_gpus=1.0,
)
class ExtractImageFeatures:
    def __init__(self):
        self.model = YOLO(YOLO_MODEL)
        if torch.cuda.is_available():
            self.model.to("cuda")

    def to_features(self, res):
        return [
            {
                "label": label,
                "confidence": confidence.item(),
                "bbox": bbox.tolist(),
            }
            for label, confidence, bbox in zip(
                res.names, res.boxes.conf, res.boxes.xyxy
            )
        ]

    def __call__(self, images):
        if len(images) == 0:
            return []
        batch = [
            torchvision.transforms.functional.to_tensor(Image.fromarray(image))
            for image in images
        ]
        stack = torch.stack(batch, dim=0)
        return daft.Series.from_pylist(
            [self.to_features(res) for res in self.model(stack)]
        )


daft.context.set_runner_ray()

df = daft.read_video_frames(
    INPUT_PATH,
    image_height=IMAGE_HEIGHT,
    image_width=IMAGE_WIDTH,
)
df = df.with_column("features", ExtractImageFeatures(col("data")))
df = df.explode("features")
df = df.with_column(
    "object",
    daft.col("data").image.crop(daft.col("features")["bbox"]).image.encode("png"),
)
df = df.exclude("data")
df.write_parquet(OUTPUT_PATH)
