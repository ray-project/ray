import argparse
import time
import uuid
from typing import Dict

import numpy as np
import torch
from benchmark import (
    Benchmark,
    BenchmarkMetric,
    RuntimeEnvSetupTracker,
    collect_dataset_stats,
    benchmark_py_modules,
)
from torchvision.models import ResNet50_Weights, resnet50

import ray
from ray.data import ActorPoolStrategy

WRITE_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"


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
    parser.add_argument(
        "--chaos-test",
        action="store_true",
        default=False,
    )
    return parser.parse_args()


def main(args):
    data_directory: str = args.data_directory
    data_format: str = args.data_format
    smoke_test: bool = args.smoke_test
    chaos_test: bool = args.chaos_test
    data_url = f"s3://anonymous@air-example-data-2/{data_directory}"

    print(f"Running GPU batch prediction with data from {data_url}")

    # Largest batch that can fit on a T4.
    INFERENCE_BATCH_SIZE = 900

    device = "cpu" if smoke_test else "cuda"

    weights = ResNet50_Weights.DEFAULT
    model = resnet50(weights=weights)
    model_ref = ray.put(model)

    # Get the preprocessing transforms from the pre-trained weights.
    transform = weights.transforms()

    if smoke_test:
        compute = ActorPoolStrategy(size=4)
        num_gpus = 0
    else:
        compute = None
        num_gpus = 1

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

    holder = {}

    def benchmark_fn():
        url = data_url
        if data_format == "raw":
            if smoke_test:
                url += "/dog_1.jpg"
            ds = ray.data.read_images(url, size=(256, 256))
        elif data_format == "parquet":
            if smoke_test:
                url += "/8cc8856e16c343829ef320fef4b353b1_000000.parquet"
            ds = ray.data.read_parquet(url)

        # Secondary timer that excludes the read_* metadata fetch.
        start_time_without_metadata_fetching = time.time()

        ds = ds.map_batches(preprocess, batch_size="auto")
        ds = ds.map_batches(
            Predictor,
            batch_size=INFERENCE_BATCH_SIZE,
            compute=compute,
            num_gpus=num_gpus,
            fn_constructor_kwargs={"model": model_ref},
        )
        ds.write_parquet(WRITE_PATH)

        holder["ds"] = ds
        holder["total_time_s_wo_metadata_fetch"] = (
            time.time() - start_time_without_metadata_fetching
        )

    benchmark = Benchmark()
    benchmark.run_fn("batch-inference", benchmark_fn)

    total_time = benchmark.result["batch-inference"][BenchmarkMetric.RUNTIME.value]
    total_time_without_metadata_fetch = holder["total_time_s_wo_metadata_fetch"]

    print("Total time (sec): ", total_time)
    print("Total time w/o metadata fetching (sec): ", total_time_without_metadata_fetch)

    if chaos_test:
        dead_nodes = [node["NodeID"] for node in ray.nodes() if not node["Alive"]]
        assert dead_nodes
        print(f"Total chaos killed: {dead_nodes}")

    results = collect_dataset_stats(holder["ds"])
    results.update(
        {
            "data_directory": data_directory,
            "data_format": data_format,
            "total_time_s_wo_metadata_fetch": total_time_without_metadata_fetch,
            "runtime_env_setup": RuntimeEnvSetupTracker.collect(),
        }
    )
    benchmark.result["batch-inference"].update(results)
    benchmark.write_result()


if __name__ == "__main__":
    args = parse_args()
    ray.init(runtime_env={"py_modules": benchmark_py_modules()})
    main(args)
