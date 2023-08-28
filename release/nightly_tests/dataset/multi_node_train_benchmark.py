from collections import defaultdict
import ray
from ray import train
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer

from benchmark import Benchmark, BenchmarkMetric


import time
import os
import json
import torchvision
import torch

# This benchmark does the following:
# 1) Read files (images or parquet) with ray.data
# 2) Apply preprocessing with map_batches()
# 3) Train TorchTrainer on processed data
# Metrics recorded to the output file are:
# - ray.torchtrainer.fit: Throughput of the final epoch in
#   TorchTrainer.fit() (step 3 above)


def parse_args():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--file-type",
        default="image",
        type=str,
        help="Input file type; choose from: ['image', 'parquet']",
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        # Use 10 epochs and report the throughput of the last epoch, in case
        # there is warmup in the first epoch.
        default=10,
        type=int,
        help="Number of epochs to run. The throughput for the last epoch will be kept.",
    )
    parser.add_argument(
        "--num-workers",
        default=1,
        type=int,
        help="Number of workers.",
    )
    parser.add_argument(
        "--local-shuffle-buffer-size",
        default=200,
        type=int,
        help="Parameter into ds.iter_batches(local_shuffle_buffer_size=...)",
    )
    parser.add_argument(
        "--preserve-order",
        action="store_true",
        default=False,
        help="Whether to configure Train with preserve_order flag.",
    )
    args = parser.parse_args()

    if args.file_type == "image":
        args.data_root = "s3://anonymous@air-example-data-2/20G-image-data-synthetic-raw"
    elif args.file_type == "parquet":
        args.data_root = "s3://anonymous@air-example-data-2/20G-image-data-synthetic-raw-parquet"
    else:
        raise Exception(
            f"Unknown file type {args.file_type}; expected one of: ['image', 'parquet']"
        )
    return args


# Constants and utility methods for image-based benchmarks.
DEFAULT_IMAGE_SIZE = 224


def get_transform(to_torch_tensor):
    # Note(swang): This is a different order from tf.data.
    # torch: decode -> randCrop+resize -> randFlip
    # tf.data: decode -> randCrop -> randFlip -> resize
    transform = torchvision.transforms.Compose(
        [
            torchvision.transforms.RandomResizedCrop(
                size=DEFAULT_IMAGE_SIZE,
                scale=(0.05, 1.0),
                ratio=(0.75, 1.33),
            ),
            torchvision.transforms.RandomHorizontalFlip(),
        ]
        + ([torchvision.transforms.ToTensor()] if to_torch_tensor else [])
    )
    return transform


def crop_and_flip_image_batch(image_batch):
    transform = get_transform(False)
    batch_size, height, width, channels = image_batch["image"].shape
    tensor_shape = (batch_size, channels, height, width)
    image_batch["image"] = transform(
        torch.Tensor(image_batch["image"].reshape(tensor_shape))
    )
    return image_batch


def benchmark_code(args, cache_output_ds=False, cache_input_ds=False, prepartition_ds=False):
    """ 
        - cache_output_ds: Cache output dataset (ds.materialize()).
            Test dataset smaller and larger than object store memory.
        - cache_input_ds: Cache input dataset, add a preprocessing fn after ds.materialize().
            Test dataset smaller and larger than object store memory.
        - prepartition_ds: Pre-partition and cache input dataset across workers.
            Test dataset smaller and larger than object store memory.
    """
    assert sum([cache_output_ds, cache_input_ds, prepartition_ds]) <= 1, "Can only test one caching variant at a time"

    # 1) Read in data with read_images() / read_parquet()
    # TODO(scott): for extra-CPU case, pass `num_cpus=N` as ray_remote_args here?
    if args.file_type == "image":
        ray_dataset = ray.data.read_images(args.data_root)
    elif args.file_type == "parquet":
        ray_dataset = ray.data.read_parquet(args.data_root)
    else:
        raise Exception(f"Unknown file type {args.file_type}")

    if cache_input_ds:
        ray_dataset = ray_dataset.materialize()

    # 2) Preprocess data by applying transformation with map_batches()
    ray_dataset = ray_dataset.map_batches(crop_and_flip_image_batch)
    if cache_output_ds:
        ray_dataset = ray_dataset.materialize()

    def train_loop_per_worker():
        it = train.get_dataset_shard("train")

        for i in range(args.num_epochs):
            num_rows = 0
            for batch in it.iter_batches(
                batch_size=args.batch_size,
                local_shuffle_buffer_size=args.local_shuffle_buffer_size,
                prefetch_batches=10,
            ):
                num_rows += args.batch_size

    # 3) Train TorchTrainer on processed data
    options = DataConfig.default_ingest_options()
    options.preserve_order = args.preserve_order

    torch_trainer = TorchTrainer(
        train_loop_per_worker,
        datasets={"train": ray_dataset},
        scaling_config=ScalingConfig(
            num_workers=args.num_workers,
            use_gpu=True,
        ),
        dataset_config=ray.train.DataConfig(
            execution_options=options,
        ),
    )
    train_start_t = time.time()
    result = torch_trainer.fit()
    train_end_t = time.time()

    # Report the throughput of one epoch (averaged across epochs)
    runtime_one_epoch = (train_end_t - train_start_t) / args.num_epochs
    tput_one_epoch = ray_dataset.count() / runtime_one_epoch
    return {BenchmarkMetric.THROUGHPUT.value: tput_one_epoch}

if __name__ == "__main__":
    args = parse_args()
    benchmark_name = f"read_{args.file_type}_train_{args.num_workers}workers"
    if args.preserve_order:
        benchmark_name = f"{benchmark_name}_preserve_order"

    benchmark = Benchmark(benchmark_name)

    benchmark.run_fn("cache-none", benchmark_code, args=args)
    benchmark.run_fn("cache-output", benchmark_code, args=args, cache_output_ds=True)
    # benchmark.run_fn("cache-output", benchmark_code, args=args, cache_output_ds=True) # TODO: cache output w/ extra CPU nodes
    benchmark.run_fn("cache-input", benchmark_code, args=args, cache_input_ds=True)
    # benchmark.run_fn("cache-output", benchmark_code, args=args, cache_output_ds=True) # TODO: prepartition + cache inputs
    benchmark.write_result("/tmp/multi_node_train_benchmark.json")
