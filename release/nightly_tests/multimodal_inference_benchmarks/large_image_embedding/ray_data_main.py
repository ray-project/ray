from io import BytesIO
import time
from typing import Dict, Any
import uuid

import numpy as np
from PIL import Image
from pybase64 import b64decode
import torch
from transformers import ViTImageProcessor, ViTForImageClassification

import ray


INPUT_PREFIX = "s3://anonymous@ray-example-data/image-datasets/10TiB-b64encoded-images-in-parquet-v3/"
OUTPUT_PREFIX = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"

BATCH_SIZE = 1024

PROCESSOR = ViTImageProcessor(
    do_convert_rgb=None,
    do_normalize=True,
    do_rescale=True,
    do_resize=True,
    image_mean=[0.5, 0.5, 0.5],
    image_std=[0.5, 0.5, 0.5],
    resample=2,
    rescale_factor=0.00392156862745098,
    size={"height": 224, "width": 224},
)


ray.init()


@ray.remote
def warmup():
    pass


# NOTE: On a fresh Ray cluster, it can take a minute or longer to schedule the first
#       task. To ensure benchmarks compare data processing speed and not cluster startup
#       overhead, this code launches a several tasks as warmup.
ray.get([warmup.remote() for _ in range(64)])


def decode(row: Dict[str, Any]) -> Dict[str, Any]:
    image_data = b64decode(row["image"], None, True)
    image = Image.open(BytesIO(image_data)).convert("RGB")
    width, height = image.size
    return {
        "original_url": row["url"],
        "original_width": width,
        "original_height": height,
        "image": np.asarray(image),
    }


def preprocess(row: Dict[str, Any]) -> Dict[str, Any]:
    outputs = PROCESSOR(images=row["image"])["pixel_values"]
    assert len(outputs) == 1, len(outputs)
    row["image"] = outputs[0]
    return row


class Infer:
    def __init__(self):
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model = ViTForImageClassification.from_pretrained(
            "google/vit-base-patch16-224"
        ).to(self._device)

    def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        with torch.inference_mode():
            next_tensor = torch.from_numpy(batch["image"]).to(
                dtype=torch.float32, device=self._device, non_blocking=True
            )
            output = self._model(next_tensor).logits
            return {
                "original_url": batch["original_url"],
                "original_width": batch["original_width"],
                "original_height": batch["original_height"],
                "output": output.cpu().numpy(),
            }


start_time = time.time()

ds = (
    ray.data.read_parquet(INPUT_PREFIX)
    .map(decode)
    .map(preprocess)
    .map_batches(
        Infer,
        batch_size=BATCH_SIZE,
        num_gpus=1,
        concurrency=40,
    )
    .write_parquet(OUTPUT_PREFIX)
)

print("Runtime", time.time() - start_time)
