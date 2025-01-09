import os
from typing import Dict

import numpy as np
import ray
from torchvision import transforms

from benchmark import Benchmark

DATA_URI = "s3://anonymous@ray-example-data/static-videos"
NUM_FILES = 91  # 50GiB / 562.4 MiB/file ~= 91 files


def main():
    """
    Test autoscaling up a CPU-only job.

    This job reads video data from S3 and then resizes the frames.
    """
    ray.init()
    paths = [f"{DATA_URI}/000.mp4" for _ in range(NUM_FILES)]
    dataset = ray.data.read_videos(paths=paths, include_paths=True)
    dataset = dataset.map(transform_frame)
    dataset = dataset.select_columns(["path", "frame_index"])
    dataset_iter = dataset.iter_batches(batch_size=None)

    benchmark = Benchmark()
    benchmark.run_iterate_ds("main", dataset_iter)
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    benchmark.write_result(test_output_json)


def transform_frame(row: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Resize((256, 256))]
    )
    row["frame"] = transform(row["frame"])
    return row


if __name__ == "__main__":
    main()
