import json
import os
from timeit import default_timer as timer
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

DATA_URI = "s3://anonymous@antoni-test/sewer-videos"


def main():
    ray.init()
    actor_pool_size = int(ray.cluster_resources().get("GPU"))

    start_time = timer()

    dataset = (
        ray.data.read_datasource(VideoDatasource(), paths=DATA_URI, include_paths=True)
        # Thr videos are long, so we're filtering out frames like SewerAI.
        .filter(lambda row: row["frame_index"] % 5 == 0)
        .map(transform_frame)
        .map_batches(
            DetectObjects,
            compute=ray.data.ActorPoolStrategy(size=actor_pool_size),
            batch_size=4,
            num_gpus=1,
        )
    )

    num_frames = 0
    for batch in dataset.iter_batches(batch_size=None):
        num_frames += len(batch["frame_index"])

    end_time = timer()

    total_time = end_time - start_time
    throughput = num_frames / total_time

    # For structured output integration with internal tooling
    results = {
        "data_uri": DATA_URI,
        "perf_metrics": [
            {
                "perf_metric_name": "total_time_s",
                "perf_metric_value": total_time,
                "perf_metric_type": "LATENCY",
            },
            {
                "perf_metric_name": "throughput_images_s",
                "perf_metric_value": throughput,
                "perf_metric_type": "THROUGHPUT",
            },
        ],
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(results, f)

    print(results)


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
