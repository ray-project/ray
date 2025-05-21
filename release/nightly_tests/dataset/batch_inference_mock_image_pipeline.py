import copy
import itertools
import uuid
from typing import Any, Dict, List

import albumentations as A
import numpy as np
import torch
from benchmark import Benchmark
from torchvision.models import ViT_B_16_Weights, vit_b_16

import ray
from ray.data import ActorPoolStrategy, DataContext

READ_PATH = "s3://ray-benchmark-data-internal/15TiB-high-resolution-images/"
WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

PATCH_SIZE = 256

# Largest batch that can fit on a T4.
BATCH_SIZE = 1200


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
        with torch.inference_mode():
            output = self._model(
                torch.as_tensor(batch.pop("patch"), device=self._device)
            )
            batch["embedding"] = output.cpu().numpy()
            return batch


def main():
    benchmark = Benchmark()

    def benchmark_fn():
        weights = ViT_B_16_Weights.DEFAULT
        model = vit_b_16(weights=weights)
        transform = weights.transforms()
        model_ref = ray.put(model)

        # Toggle on features that are required for the pipeline to work.
        ctx = DataContext.get_current()
        ctx.enable_fallback_to_arrow_object_ext_type = True
        ctx.execution_options.actor_locality_enabled = True

        (
            ray.data.read_images(READ_PATH, mode="RGB")
            .filter(lambda row: row["image"].size != 0)
            .map(process_image)
            .flat_map(patch_image)
            .map_batches(ProcessPatches(transform))
            .map_batches(
                EmbedPatches,
                batch_size=BATCH_SIZE,
                compute=ActorPoolStrategy(min_size=1, max_size=100),
                num_gpus=1,
                fn_constructor_kwargs={"model": model_ref, "device": "cuda"},
            )
            .write_parquet(WRITE_PATH)
        )

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


if __name__ == "__main__":
    main()
