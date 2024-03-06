import os
from typing import Dict

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

DATA_URI = "s3://anonymous@antoni-test/sewer-videos"
# ceil(10GB / 56.2 MB/file) = 178 files
NUM_FILES = 178


def main():
    """Read in NUM_FILES video files from a flat S3 bucket,
    apply preprocessing on a partial set of video frames,
    and perform object detection as the batch inference task.
    Reports the time taken and throughput (# frames/second)."""
    ray.init()
    actor_pool_size = int(ray.cluster_resources().get("GPU"))
    paths = [f"{DATA_URI}/sewer_example_{i}.mp4" for i in range(NUM_FILES)]

    dataset = (
        ray.data.read_datasource(
            VideoDatasource(paths=paths, include_paths=True),
            ray_remote_args={"num_cpus": 5},
        )
        # The videos are long, so we're filtering out frames like SewerAI.
        .filter(lambda row: row["frame_index"] % 5 == 0)
        .map(transform_frame)
        .map_batches(
            DetectObjects,
            compute=ray.data.ActorPoolStrategy(size=actor_pool_size),
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


if __name__ == "__main__":
    main()
