import os
from typing import Dict
import argparse

import numpy as np
import ray
import torch
from torchvision import transforms
from torchvision.models.detection import (
    FasterRCNN_ResNet50_FPN_V2_Weights,
    fasterrcnn_resnet50_fpn_v2,
)

from ray.anyscale.data import VideoDatasource

from benchmark import Benchmark

DATA_URI = "s3://anonymous@ray-example-data/static-videos"
NUM_FILES = 91  # 50GiB / 562.4 MiB/file ~= 91 files


def main(args):
    """Read in NUM_FILES video files from a flat S3 bucket,
    apply preprocessing on a partial set of video frames,
    and perform object detection as the batch inference task.
    Reports the time taken and throughput (# frames/second)."""
    ray.init()
    if args.min_gpus is not None:
        actor_pool_concurrency = (args.min_gpus, args.max_gpus)
    else:
        actor_pool_concurrency = int(ray.cluster_resources().get("GPU"))
    paths = [f"{DATA_URI}/000.mp4" for _ in range(NUM_FILES)]
    dataset = (
        ray.data.read_datasource(VideoDatasource(paths=paths, include_paths=True))
        # The videos are long, so we're filtering out frames like SewerAI.
        .filter(lambda row: row["frame_index"] % 5 == 0)
        .map(transform_frame)
        .map_batches(
            DetectObjects,
            concurrency=actor_pool_concurrency,
            batch_size=4,
            num_gpus=1,
        )
    )
    dataset_iter = dataset.iter_batches(batch_size=None)

    benchmark = Benchmark("video_batch_inference_benchmark")
    benchmark.run_iterate_ds("main", dataset_iter)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    benchmark.write_result(test_output_json)


def transform_frame(row: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    weights = FasterRCNN_ResNet50_FPN_V2_Weights.DEFAULT
    # Convert data from uint8 to float and scale the values accordingly.
    transform = transforms.Compose([transforms.ToTensor(), weights.transforms()])
    row["frame"] = transform(row["frame"])
    return row


class DetectObjects:
    def __init__(self):
        weights = FasterRCNN_ResNet50_FPN_V2_Weights.DEFAULT
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model = fasterrcnn_resnet50_fpn_v2(weights=weights, box_score_thresh=0.9)
        self.model.to(self.device)
        self.model.eval()

    def __call__(self, batch: Dict[str, np.ndarray]):
        inputs = [torch.from_numpy(frame).to(self.device) for frame in batch["frame"]]
        with torch.inference_mode():
            outputs = self.model(inputs)
        return {
            "path": batch["path"],
            "frame_index": batch["frame_index"],
            "labels": [output["labels"].detach().cpu().numpy() for output in outputs],
            "boxes": [output["boxes"].detach().cpu().numpy() for output in outputs],
        }


def parse_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--min-gpus",
        required=False,
        type=int,
        help="Minimum number of GPUs to use for batch inference.",
    )
    parser.add_argument(
        "--max-gpus",
        required=False,
        type=int,
        help="Maximum number of GPUs to use for batch inference.",
    )
    args = parser.parse_args()
    if args.min_gpus is not None:
        assert args.min_gpus > 0
    if args.max_gpus is not None:
        assert args.min_gpus is not None
        assert args.max_gpus > args.min_gpus
    return args


if __name__ == "__main__":
    main(parse_cli_args())
