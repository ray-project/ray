import argparse
import uuid
from io import BytesIO
from typing import Dict, List, Any

import numpy as np
import ray
import torch
from transformers import ViTImageProcessor, ViTForImageClassification
from PIL import Image
from pybase64 import b64decode

from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray._private.test_utils import EC2InstanceTerminatorWithGracePeriod
from benchmark import Benchmark


INPUT_PREFIX = "s3://ray-benchmark-data-internal/10TiB-jsonl-images"
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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--inference-concurrency",
        nargs=2,
        type=int,
        required=True,
        help="The minimum and maximum concurrency for the inference operator.",
    )
    parser.add_argument(
        "--chaos",
        action="store_true",
        help=(
            "Whether to enable chaos. If set, this script terminates one worker node "
            "every minute with a grace period."
        ),
    )
    return parser.parse_args()


def main(args: argparse.Namespace):
    benchmark = Benchmark()

    if args.chaos:
        start_chaos()

    def benchmark_fn():
        (
            ray.data.read_json(INPUT_PREFIX, lines=True)
            .flat_map(decode)
            .map(preprocess)
            .map_batches(
                Infer,
                batch_size=BATCH_SIZE,
                num_gpus=1,
                concurrency=tuple(args.inference_concurrency),
            )
            .write_parquet(OUTPUT_PREFIX)
        )

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()


def start_chaos():
    assert ray.is_initialized()

    head_node_id = ray.get_runtime_context().get_node_id()
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        node_id=head_node_id, soft=False
    )
    resource_killer = EC2InstanceTerminatorWithGracePeriod.options(
        scheduling_strategy=scheduling_strategy
    ).remote(head_node_id, max_to_kill=None)

    ray.get(resource_killer.ready.remote())

    resource_killer.run.remote()


def decode(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    image_data = b64decode(row["image"], None, True)
    image = Image.open(BytesIO(image_data))
    width, height = image.size
    return [
        {
            "original_url": row["url"],
            "original_width": width,
            "original_height": height,
            "image": np.asarray(image),
        }
    ]


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


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
