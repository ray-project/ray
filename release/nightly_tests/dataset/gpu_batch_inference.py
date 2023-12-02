from typing import Dict
import argparse
import time

import numpy as np
import torch
from torchvision.models import resnet50, ResNet50_Weights

from benchmark import Benchmark, BenchmarkMetric
import ray
from ray.data import ActorPoolStrategy


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-directory",
        help=(
            "Name of the S3 directory in the air-example-data-2 "
            "bucket to load data from."
        ),
    )
    parser.add_argument(
        "--data-format",
        choices=["parquet", "raw"],
        help="The format of the data. Can be either parquet or raw.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


def main(args):
    data_directory: str = args.data_directory
    data_format: str = args.data_format
    smoke_test: bool = args.smoke_test
    data_url = f"s3://anonymous@air-example-data-2/{data_directory}"

    print(f"Running GPU batch prediction with data from {data_url}")

    # Largest batch that can fit on a T4.
    BATCH_SIZE = 1000

    device = "cpu" if smoke_test else "cuda"

    weights = ResNet50_Weights.DEFAULT
    model = resnet50(weights=weights)
    model_ref = ray.put(model)

    # Get the preprocessing transforms from the pre-trained weights.
    transform = weights.transforms()

    start_time = time.time()

    if data_format == "raw":
        if smoke_test:
            data_url += "/dog_1.jpg"
        ds = ray.data.read_images(data_url, size=(256, 256))
    elif data_format == "parquet":
        if smoke_test:
            data_url += "/8cc8856e16c343829ef320fef4b353b1_000000.parquet"
        ds = ray.data.read_parquet(data_url)

    # Preprocess the images using standard preprocessing
    def preprocess(image_batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        tensor_batch = torch.as_tensor(image_batch["image"], dtype=torch.float)
        # (B, H, W, C) -> (B, C, H, W). This is required for the torchvision transform.
        # https://pytorch.org/vision/main/models/generated/torchvision.models.resnet50.html#torchvision.models.ResNet50_Weights  # noqa
        tensor_batch = tensor_batch.permute(0, 3, 1, 2)
        transformed_batch = transform(tensor_batch).numpy()
        return {"image": transformed_batch}

    class Predictor:
        def __init__(self, model):
            self.model = ray.get(model)
            self.model.eval()
            self.model.to(device)

        def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            with torch.inference_mode():
                output = self.model(torch.as_tensor(batch["image"], device=device))
                return {"predictions": output.cpu().numpy()}

    start_time_without_metadata_fetching = time.time()

    if smoke_test:
        actor_pool_size = 4
        num_gpus = 0
    else:
        actor_pool_size = int(ray.cluster_resources().get("GPU"))
        num_gpus = 1
    ds = ds.map_batches(preprocess)
    ds = ds.map_batches(
        Predictor,
        batch_size=BATCH_SIZE,
        compute=ActorPoolStrategy(size=actor_pool_size),
        num_gpus=num_gpus,
        fn_constructor_kwargs={"model": model_ref},
        max_concurrency=2,
    )

    # Force execution.
    total_images = 0
    for batch in ds.iter_batches(batch_size=None, batch_format="pyarrow"):
        total_images += len(batch)
    end_time = time.time()

    total_time = end_time - start_time
    throughput = total_images / (total_time)
    total_time_without_metadata_fetch = end_time - start_time_without_metadata_fetching
    throughput_without_metadata_fetch = total_images / (
        total_time_without_metadata_fetch
    )

    print("Total time (sec): ", total_time)
    print("Throughput (img/sec): ", throughput)
    print("Total time w/o metadata fetching (sec): ", total_time_without_metadata_fetch)
    print(
        "Throughput w/o metadata fetching (img/sec): ",
        throughput_without_metadata_fetch,
    )

    # For structured output integration with internal tooling
    results = {
        BenchmarkMetric.RUNTIME: total_time,
        BenchmarkMetric.THROUGHPUT: throughput,
        "data_directory": data_directory,
        "data_format": data_format,
        "total_time_s_wo_metadata_fetch": total_time_without_metadata_fetch,
        "throughput_images_s_wo_metadata_fetch": throughput_without_metadata_fetch,
    }

    return results


if __name__ == "__main__":
    args = parse_args()

    benchmark = Benchmark("gpu-batch-inference")
    benchmark.run_fn("batch-inference", main, args)
    benchmark.write_result()
