import argparse
import io
import uuid
from typing import Any, Dict

import boto3
import numpy as np
import pandas as pd
import torch
from benchmark import Benchmark
from PIL import Image
from torchvision.models import ResNet50_Weights, resnet50

import ray
from ray.data import ActorPoolStrategy

BUCKET = "anyscale-imagenet"
# This Parquet file contains the keys of images in the 'anyscale-imagenet' bucket.
METADATA_PATH = "s3://anyscale-imagenet/metadata.parquet"

# Largest batch that can fit on a T4.
BATCH_SIZE = 900

WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sf",
        dest="scale_factor",
        type=int,
        default=1,
        help=(
            "The number of copies of ImageNet to read. Use this to simulate a larger "
            "dataset."
        ),
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    metadata = pd.read_parquet(METADATA_PATH)
    # Repeat the metadata 'scale_factor' times to simulate a larger dataset.
    metadata = pd.concat([metadata] * args.scale_factor, ignore_index=True)

    def benchmark_fn():
        weights = ResNet50_Weights.DEFAULT
        model = resnet50(weights=weights)
        model_ref = ray.put(model)

        # Get the preprocessing transforms from the pre-trained weights.
        transform = weights.transforms()

        (
            ray.data.from_pandas(metadata)
            # TODO: There should be a way to specify "use as many actors as possible"
            # with the now-recommended `concurrency` parameter.
            .map(LoadImage, compute=ActorPoolStrategy(min_size=1))
            # Preprocess the images using standard preprocessing
            .map(ApplyTransform(transform))
            .map_batches(
                Predictor,
                batch_size=BATCH_SIZE,
                compute=ActorPoolStrategy(min_size=1),
                num_gpus=1,
                fn_constructor_kwargs={"model": model_ref, "device": "cuda"},
            )
            .write_parquet(WRITE_PATH)
        )

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


class LoadImage:
    def __init__(self):
        self._client = boto3.client("s3")

    def __call__(self, row):
        data = io.BytesIO()
        self._client.download_fileobj(BUCKET, row["key"], data)
        image = Image.open(data).convert("RGB")
        return {"image": np.array(image)}


class ApplyTransform:
    def __init__(self, transform):
        self._transform = transform

    def __call__(self, row: Dict[str, Any]) -> Dict[str, Any]:
        # 'row["image"]' isn't writeable, and Torch only supports writeable tensors, so
        # we need to maky a copy to prevent Torch from complaining.
        tensor_batch = torch.as_tensor(np.copy(row["image"]), dtype=torch.float)
        # (H, W, C) -> (C, H, W). This is required for the torchvision transform.
        # https://pytorch.org/vision/main/models/generated/torchvision.models.resnet50.html#torchvision.models.ResNet50_Weights  # noqa: E501
        tensor_batch = tensor_batch.permute(2, 0, 1)
        transformed_batch = self._transform(tensor_batch).numpy()
        return {"image": transformed_batch}


class Predictor:
    def __init__(self, model, device):
        self._model = ray.get(model)
        self._model.eval()
        self._model.to(device)

        self._device = device

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with torch.inference_mode():
            output = self._model(torch.as_tensor(batch["image"], device=self._device))
            return {"predictions": output.cpu().numpy()}


if __name__ == "__main__":
    args = parse_args()
    main(args)
