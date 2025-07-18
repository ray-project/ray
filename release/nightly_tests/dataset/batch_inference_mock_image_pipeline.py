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
from torchvision.models import vit_b_16, ViT_B_16_Weights
import albumentations as A
import ray
from ray.data import ActorPoolStrategy, DataContext
import copy
import itertools
from typing import List
import string
import random
import time

WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
BUCKET = "ray-benchmark-data-internal-us-west-2"

# Assumptions: homogenously shaped images, homogenous images
# Each iamge is 2048 * 2048 * 3 = 12.58 MB -> 11 images / block. 8 blocks per task, so ~88 images per task.
IMAGES_PER_BLOCK = 11
BLOCKS_PER_TASK = 8
NUM_UNITS = 1380
NUM_CONTAINERS = 50
OVERRIDE_NUM_BLOCKS = int(NUM_CONTAINERS * NUM_UNITS / IMAGES_PER_BLOCK)
PATCH_SIZE = 256

# Largest batch that can fit on a T4.
BATCH_SIZE = 1200

# On a T4 GPU, it takes ~11.3s to perform inference on 1200 images. So, the time per
# image is 11.3s / 1200 ~= 0.0094s.
INFERENCE_LATENCY_PER_IMAGE_S = 0.0094


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sf",
        dest="scale_factor",
        type=int,
        default=1,
        help=(
            "The number of copies of the dataset to read. Use this to simulate a larger "
            "dataset."
        ),
    )
    return parser.parse_args()


def create_metadata(scale_factor: int):
    # TODO(mowen): Handle repeats of the dataset if scale_factor > 1
    # simulate various text metadata fields alongside image metadata
    return pd.DataFrame(
        [
            {
                "metadata_0": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_1": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_2": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_3": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_4": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_5": "".join(random.choices(string.ascii_letters, k=16)),
                "metadata_6": "".join(random.choices(string.ascii_letters, k=16)),
                "container_order_read_id": f"{i:04d}_{j:04d}",
                "container_id": i,
                "channel_keys": [
                    f"15TiB-high-resolution-images/group={i:04d}/{j:04d}_{k}.png"
                    for k in range(3)
                ],
                "applied_scale": 1,
            }
            for j in range(NUM_UNITS)
            for i in range(NUM_CONTAINERS)
        ]
    )


class LoadImage:
    def __init__(self):
        self._client = boto3.client("s3")

    def __call__(self, row):
        channels = []
        for key in row["channel_keys"]:
            data = io.BytesIO()
            self._client.download_fileobj(BUCKET, key, data)
            image = Image.open(data)
            channels.append(np.array(image))

        row["image"] = np.dstack(channels)
        return row


def process_image(row: Dict[str, Any]) -> Dict[str, np.ndarray]:
    transform = A.Compose(
        [
            A.ToFloat(),
            A.LongestMaxSize(
                max_size=int(row["image"].shape[0] * float(1.0 / row["applied_scale"]))
            ),
            A.FromFloat(dtype="uint8"),
        ]
    )
    row["image"] = transform(image=row["image"])["image"]
    return row


def patch_image(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    image = row.pop("image")

    patches = []
    width, height, _ = image.shape
    for x, y in itertools.product(
        range(PATCH_SIZE, width - PATCH_SIZE, PATCH_SIZE),
        range(PATCH_SIZE, height - PATCH_SIZE, PATCH_SIZE),
    ):
        patch = image[y : y + PATCH_SIZE, x : x + PATCH_SIZE, :]

        patch_row = copy.deepcopy(row)
        patch_row["patch_x"] = x
        patch_row["patch_y"] = y
        patch_row["patch_width"] = PATCH_SIZE
        patch_row["patch_height"] = PATCH_SIZE
        patch_row["patch"] = patch

        patches.append(patch_row)

    return patches


class ProcessPatches:
    def __init__(self, transform):
        self._transform = transform

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["patch"] = self._transform(
            torch.as_tensor(batch["patch"]).permute(0, 3, 1, 2)
        )
        return batch


class EmbedPatches:
    def __init__(self, model, device):
        self._model = ray.get(model)
        self._model.eval()
        self._model.to(device)
        self._device = device

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.as_tensor(batch.pop("patch"), device=self._device)
        with torch.inference_mode():
            output = self._model(inputs)
            batch["embedding"] = output.cpu().numpy()
            return batch


class FakeEmbedPatches:
    def __init__(self, model, device):
        self._model = ray.get(model)
        self._model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        inputs = torch.as_tensor(batch.pop("patch"))
        with torch.inference_mode():
            # Simulate inference latency with a sleep
            time.sleep(INFERENCE_LATENCY_PER_IMAGE_S * len(inputs))
            # Generate fake embeddings
            output = torch.rand((len(inputs), 1000), dtype=torch.float)
            batch["embedding"] = output.cpu().numpy()
            return batch


def main(scale_factor: int):
    benchmark = Benchmark()

    print("Creating metadata")
    metadata = create_metadata(scale_factor=scale_factor)

    def benchmark_fn():
        weights = ViT_B_16_Weights.DEFAULT
        model = vit_b_16(weights=weights)
        transform = weights.transforms()
        model_ref = ray.put(model)

        # Toggle on features that are required for the pipeline to work.
        ctx = DataContext.get_current()
        ctx.enable_fallback_to_arrow_object_ext_type = True
        ctx.execution_options.actor_locality_enabled = True

        print(f"Starting pipeline with {OVERRIDE_NUM_BLOCKS} blocks")
        (
            ray.data.from_pandas(metadata, override_num_blocks=OVERRIDE_NUM_BLOCKS)
            .map(
                LoadImage,
                # TODO(mowen): When we fix the deadlocking bug we should increase this to 800.
                compute=ActorPoolStrategy(min_size=1, max_size=700),
                max_concurrency=4,  # needed to prevent image loading from becoming the bottleneck
            )
            .filter(lambda row: row["image"].size != 0)
            .map(process_image)
            .flat_map(patch_image)
            .map_batches(ProcessPatches(transform))
            .map_batches(
                FakeEmbedPatches,
                batch_size=BATCH_SIZE,
                compute=ActorPoolStrategy(min_size=1, max_size=100),
                fn_constructor_kwargs={"model": model_ref, "device": "cuda"},
            )
            .write_parquet(WRITE_PATH)
        )

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    scale_factor = args.scale_factor
    main(scale_factor)
