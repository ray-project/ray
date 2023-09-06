import ray
from ray import train
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer
import os

import torch.distributed as dist
import numpy as np
import math

from benchmark import Benchmark, BenchmarkMetric

from image_loader_microbenchmark import get_mosaic_dataloader, get_ray_parquet_dataset


import time
import torchvision
import torch


# This benchmark does the following:
# 1) Read files (images or parquet) with ray.data
# 2) Apply preprocessing with map_batches()
# 3) Train TorchTrainer on processed data
# Metrics recorded to the output file are:
# - ray.torchtrainer.fit: Throughput of the final epoch in
#   TorchTrainer.fit() (step 3 above)


def get_prop_parquet_paths(num_workers, target_worker_gb=10):
    parquet_s3_root = "s3://anyscale-imagenet/parquet/d76458f84f2544bdaac158d1b6b842da"
    mb_per_file = 128
    TARGET_NUM_FILES = math.ceil(target_worker_gb * num_workers * 1024 / mb_per_file)
    file_paths = []
    for fi in range(200):
        for i in range(5):
            if not (fi in [163, 164, 174, 181, 183, 190] and i == 4):
                # for some files, they only have 4 shards instead of 5.
                file_paths.append(f"{parquet_s3_root}_{fi:06}_{i:06}.parquet")
            if len(file_paths) >= TARGET_NUM_FILES:
                break
        if len(file_paths) >= TARGET_NUM_FILES:
            break
    return file_paths

def get_mosaic_epoch_size(num_workers, target_worker_gb=10):
    if target_worker_gb == -1:
        return None
    AVG_MOSAIC_IMAGE_SIZE_BYTES = 500 * 1024  # 500KiB.
    epoch_size = math.ceil(target_worker_gb * num_workers * 1024 * 1024 * 1024 / AVG_MOSAIC_IMAGE_SIZE_BYTES)
    return epoch_size


def parse_args():
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("--data-root", type=str, help="Root of data directory")
    parser.add_argument(
        "--read-local",
        action="store_true",
        default=False,
        help="Whether to read from local fs for default datasource (S3 otherwise)",
    )
    parser.add_argument(
        "--file-type",
        default="image",
        type=str,
        help="Input file type; choose from: ['image', 'parquet']",
    )
    parser.add_argument(
        "--repeat-ds",
        default=1,
        type=int,
        help="Read the input dataset n times, used to increase the total data size.",
    )
    parser.add_argument(
        "--read-task-cpus",
        default=1,
        type=int,
        help="Number of CPUs specified for read task",
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        # Use 3 epochs and report the throughput of the last epoch, in case
        # there is warmup in the first epoch.
        default=3,
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
        "--use-gpu",
        action="store_true",
        default=False,
        help="Whether to use GPU with TorchTrainer.",
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
    parser.add_argument(
        "--use-mosaic",
        action="store_true",
        default=False,
        help="",
    )
    parser.add_argument(
        "--torch-num-workers",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--target-worker-gb",
        default=10,
        type=int,
        help="Target amount of data to read per Train worker. -1 means the whole dataset",
    )
    args = parser.parse_args()

    if args.data_root is None and not args.use_mosaic:
        # use default datasets if data root is not provided
        if args.file_type == "image":
            # 1GB ragged dataset
            args.data_root = "s3://imagenetmini1000/1gb/train/"

            # Alternative larger dataset
            # args.data_root = "s3://air-example-data-2/10G-image-data-synthetic-raw"  # noqa: E501

        elif args.file_type == "parquet":
            args.data_root = (
                "s3://air-example-data-2/20G-image-data-synthetic-raw-parquet"
            )
        else:
            raise Exception(
                f"Unknown file type {args.file_type}; "
                "expected one of: ['image', 'parquet']"
            )
        if args.repeat_ds > 1:
            args.data_root = [args.data_root] * args.repeat_ds
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


def crop_and_flip_image(row):
    transform = get_transform(False)
    # Make sure to use torch.tensor here to avoid a copy from numpy.
    row["image"] = transform(torch.tensor(np.transpose(row["image"], axes=(2, 0, 1))))
    return row


def train_loop_per_worker():
    it = train.get_dataset_shard("train")
    device = train.torch.get_device()

    if args.use_mosaic:
        target_epoch_size = get_mosaic_epoch_size(args.num_workers, target_worker_gb=args.target_worker_gb)
        print("Epoch size:", target_epoch_size if target_epoch_size is not None else "all", "images")

        num_physical_nodes = ray.train.get_context().get_world_size() // ray.train.get_context().get_local_world_size()
        torch_num_workers = args.torch_num_workers
        if torch_num_workers is None:
            torch_num_workers = os.cpu_count()
        # Divide by the number of Train workers because each has its own dataloader.
        torch_num_workers //= ray.train.get_context().get_local_world_size()

        torch_iter = get_mosaic_dataloader(
            args.data_root,
            batch_size=args.batch_size,
            num_physical_nodes=num_physical_nodes,
            epoch_size=target_epoch_size,
            num_workers=torch_num_workers,
            )
    else:
        torch_iter = None

    for i in range(args.num_epochs):
        print(f"Epoch {i+1} of {args.num_epochs}")
        num_rows = 0
        start_t = time.time()

        if isinstance(it, ray.data.iterator.DataIterator):
            torch_iter = it.iter_torch_batches(
                batch_size=args.batch_size,
            )

        print_at_interval = 1000
        print_at = print_at_interval
        for batch in torch_iter:
            num_rows += args.batch_size
            if num_rows >= print_at:
                print(f"Read {num_rows} rows on rank {train.get_context().get_world_rank()}, tput so far: {num_rows / (time.time()  - start_t)}")
                print_at = ((num_rows // print_at_interval) + 1) * print_at_interval
        end_t = time.time()
        print(f"Epoch {i+1} of {args.num_epochs}, tput: {num_rows / (end_t - start_t)}, run time: {end_t - start_t}")

    # Workaround to report the final epoch time from each worker, so that we
    # can sum up the times at the end when calculating throughput.
    world_size = ray.train.get_context().get_world_size()
    all_workers_time_list = [
        torch.zeros((2), dtype=torch.double, device=device) for _ in range(world_size)
    ]
    curr_worker_time = torch.tensor([start_t, end_t], dtype=torch.double, device=device)
    dist.all_gather(all_workers_time_list, curr_worker_time)

    all_num_rows = [
        torch.zeros((1), dtype=torch.int32, device=device) for _ in range(world_size)
    ]
    curr_num_rows = torch.tensor([num_rows], dtype=torch.int32, device=device)
    dist.all_gather(all_num_rows, curr_num_rows)

    train.report(
        {
            "time_final_epoch": [tensor.tolist() for tensor in all_workers_time_list],
            "num_rows": [tensor.item() for tensor in all_num_rows],
        }
    )


def benchmark_code(
    args,
    cache_output_ds=False,
    cache_input_ds=False,
):
    """
    - cache_output_ds: Cache output dataset (ds.materialize()) after preprocessing fn.
    - cache_input_ds: Cache input dataset, then apply a preprocessing fn.
    """
    assert (
        sum([cache_output_ds, cache_input_ds]) <= 1
    ), "Can only test one caching variant at a time"

    ray_dataset = None
    # 1) Read in data with read_images() / read_parquet()
    if not args.use_mosaic:
        if args.file_type == "image":
            ray_dataset = ray.data.read_images(
                args.data_root,
                mode="RGB",
            )
        elif args.file_type == "parquet":
            # ray_dataset = ray.data.read_parquet(
            #     args.data_root,
            # )
            args.data_root = get_prop_parquet_paths(num_workers=args.num_workers)
            ray_dataset = get_ray_parquet_dataset(args.data_root)
        else:
            raise Exception(f"Unknown file type {args.file_type}")

    # if cache_input_ds:
    #     ray_dataset = ray_dataset.materialize()

    # # 2) Preprocess data by applying transformation with map/map_batches()
    # ray_dataset = ray_dataset.map(crop_and_flip_image)
    # if cache_output_ds:
    #     ray_dataset = ray_dataset.materialize()

    # 3) Train TorchTrainer on processed data
    options = DataConfig.default_ingest_options()
    options.preserve_order = args.preserve_order

    if args.num_workers == 1 or args.use_gpu:
        torch_trainer = TorchTrainer(
            train_loop_per_worker,
            datasets={"train": ray_dataset} if ray_dataset is not None else {},
            scaling_config=ScalingConfig(
                num_workers=args.num_workers,
                use_gpu=args.use_gpu,
            ),
            dataset_config=ray.train.DataConfig(
                execution_options=options,
            ),
        )
    else:
        torch_trainer = TorchTrainer(
            train_loop_per_worker,
            datasets={"train": ray_dataset} if ray_dataset is not None else {},
            # In the multi-node case without GPUs, we use a SPREAD placement strategy
            # to ensure that tasks are spread across nodes. We reserve one worker
            # for the driver.
            scaling_config=ScalingConfig(
                num_workers=args.num_workers,
                use_gpu=args.use_gpu,
            ),
            dataset_config=ray.train.DataConfig(
                execution_options=options,
            ),
        )

    result = torch_trainer.fit()

    # Report the throughput of the last epoch, sum runtime across all workers.
    time_start_last_epoch, time_end_last_epoch = zip(
        *result.metrics["time_final_epoch"]
    )
    runtime_last_epoch = max(time_end_last_epoch) - min(time_start_last_epoch)
    num_rows_last_epoch = sum(result.metrics["num_rows"])
    print("Total num rows read in last epoch:", num_rows_last_epoch, "images")
    print("Runtime last epoch:", runtime_last_epoch, "seconds")
    tput_last_epoch = num_rows_last_epoch / runtime_last_epoch

    return {
        BenchmarkMetric.THROUGHPUT.value: tput_last_epoch,
    }


if __name__ == "__main__":
    args = parse_args()
    benchmark_name = (
        f"read_{args.file_type}_repeat{args.repeat_ds}_train_{args.num_workers}workers_{args.target_worker_gb}gb_per_worker"
    )
    if args.preserve_order:
        benchmark_name = f"{benchmark_name}_preserve_order"

    benchmark = Benchmark(benchmark_name)

    benchmark.run_fn("cache-none", benchmark_code, args=args)
    # benchmark.run_fn("cache-output", benchmark_code, args=args, cache_output_ds=True)
    # benchmark.run_fn("cache-input", benchmark_code, args=args, cache_input_ds=True)
    # # TODO: enable after implementing prepartition case.
    # # benchmark.run_fn(
    # # "prepartition-ds", benchmark_code, args=args, prepartition_ds=True,
    # # )
    benchmark.write_result("/tmp/multi_node_train_benchmark.json")