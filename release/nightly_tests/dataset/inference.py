import json
import os
import time
from typing import Any, Dict

import numpy as np
import torch
from torchvision import transforms
from torchvision.models import resnet50

import ray


class ImageClassifier:
    def __init__(self):
        self.model = resnet50(pretrained=True).eval().half().cuda()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.from_numpy(batch["image"]).half().cuda()
        with torch.no_grad():
            outputs = self.model(inputs)
            predictions = torch.argmax(outputs, dim=1).cpu()
        batch["predictions"] = predictions
        return batch


transform = transforms.Compose(
    [
        transforms.ToTensor(),
        transforms.Resize(224),
        transforms.CenterCrop(224),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ]
)


def preprocess(record: Dict[str, Any]) -> Dict[str, Any]:
    record["image"] = transform(record["image"])
    return record


start_time = time.time()

ds = (
    ray.data.read_images(
        "s3://anyscale-data/small-images/",
        parallelism=1000,
        ray_remote_args={"num_cpus": 0.5},
        mode="RGB",
    )
    .map(preprocess)
    .map_batches(
        ImageClassifier,
        num_gpus=0.25,
        batch_size=128,
        compute=ray.data.ActorPoolStrategy(),
    )
    .materialize()
)

total_time = time.time() - start_time

print("total time", total_time)

if "TEST_OUTPUT_JSON" in os.environ:
    out_file = open(os.environ["TEST_OUTPUT_JSON"], "w")
    results = {
        "total_time": total_time,
    }
    json.dump(results, out_file)
