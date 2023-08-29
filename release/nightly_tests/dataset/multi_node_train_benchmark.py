from collections import defaultdict
import ray
from ray import train
from ray.actor import ActorHandle
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.data._internal.execution.interfaces import NodeIdStr
from ray.data._internal.execution.interfaces.execution_options import ExecutionOptions
from ray.data.dataset import Dataset
from ray.data.iterator import DataIterator

import torch.distributed as dist

from benchmark import Benchmark, BenchmarkMetric


import time
import torchvision
import torch

from typing import Union, Literal, List, Optional, Dict

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
        "--data-root",
        type=str,
        help="Root of data directory"
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
        help="Read the input dataset n times, used to increase the total data size."
    )
    parser.add_argument(
        "--read-task-cpus",
        default=1,
        type=int,
        help="Number of CPUs specified for read task"
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

    if args.data_root is None:
        # use default datasets if data root is not provided
        if args.file_type == "image":
            args.data_root = "s3://anonymous@air-example-data-2/20G-image-data-synthetic-raw"
            # args.data_root = "s3://air-cuj-imagenet-1gb"
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


def benchmark_code(args, read_num_cpus=None, cache_output_ds=False, cache_input_ds=False, prepartition_ds=False):
    """ 
        - cache_output_ds: Cache output dataset (ds.materialize()).
            Test dataset smaller and larger than object store memory.
        - cache_input_ds: Cache input dataset, add a preprocessing fn after ds.materialize().
            Test dataset smaller and larger than object store memory.
        - prepartition_ds: Pre-partition and cache input dataset across workers.
            Test dataset smaller and larger than object store memory.
    """
    assert sum([cache_output_ds, cache_input_ds, prepartition_ds]) <= 1, "Can only test one caching variant at a time"

    # start_t = time.time()
    # 1) Read in data with read_images() / read_parquet()
    read_ray_remote_args = {}
    if read_num_cpus is not None:
        read_ray_remote_args.update({"num_cpus": read_num_cpus})

    if args.file_type == "image":
        ray_dataset = ray.data.read_images(
            args.data_root,
            ray_remote_args=read_ray_remote_args,
        )
    elif args.file_type == "parquet":
        ray_dataset = ray.data.read_parquet(
            args.data_root,
            ray_remote_args=read_ray_remote_args,
        )
    else:
        raise Exception(f"Unknown file type {args.file_type}")
    
    if args.repeat_ds > 1:
        ray_dataset = ray_dataset.union(*[Dataset.copy(ray_dataset) for _ in range(args.repeat_ds-1)])

    if cache_input_ds:
        ray_dataset = ray_dataset.materialize()

    # 2) Preprocess data by applying transformation with map_batches()
    ray_dataset = ray_dataset.map_batches(crop_and_flip_image_batch, batch_size=args.batch_size)
    if cache_output_ds:
        ray_dataset = ray_dataset.materialize()

    def train_loop_per_worker():
        it = train.get_dataset_shard("train")

        for i in range(args.num_epochs):
            print(f"Epoch {i+1} of {args.num_epochs}")
            num_rows = 0
            start_t = time.time()
            for batch in it.iter_batches(
                batch_size=args.batch_size,
                # local_shuffle_buffer_size=args.local_shuffle_buffer_size,
                prefetch_batches=10,
            ):
                num_rows += args.batch_size
            end_t = time.time()
        # Workaround to report the final epoch time from each worker, so that we
        # can sum up the times at the end when calculating throughput.
        world_size = ray.train.get_context().get_world_size()
        all_workers_time_list = [torch.zeros((2), dtype=torch.double) for _ in range(world_size)]
        curr_worker_time = torch.tensor([start_t, end_t], dtype=torch.double)
        dist.all_gather(all_workers_time_list, curr_worker_time)
        train.report({
            f"time_final_epoch": [tensor.tolist() for tensor in all_workers_time_list],
        })

    # 3) Train TorchTrainer on processed data
    options = DataConfig.default_ingest_options()
    options.preserve_order = args.preserve_order

    if prepartition_ds:
        class PrepartitionCacheDataConfig(DataConfig):
            """Instead of using streaming_split to split the dataset amongst workers,
            pre-partition using Dataset.split(), cache the materialized shards,
            and assign to each worker."""
            def __init__(
                self,
                datasets_to_split: Union[Literal["all"], List[str]] = "all",
                execution_options: Optional[ExecutionOptions] = None,
            ):
                super().__init__(datasets_to_split, execution_options)
        
            def configure(
                self,
                datasets: Dict[str, Dataset],
                world_size: int,
                worker_handles: Optional[List[ActorHandle]],
                worker_node_ids: Optional[List[NodeIdStr]],
                **kwargs,
            ) -> List[Dict[str, DataIterator]]:
                assert len(datasets) == 1, "This example only handles the simple case"

                # Split the dataset into shards, materializing the results.
                materialized_shards = datasets["train"].split(
                    world_size, locality_hints=worker_handles
                )

                # Return the assigned shards for each worker.
                return [{"train": s} for s in materialized_shards]

        dataset_config_cls = PrepartitionCacheDataConfig
    else:
        dataset_config_cls = ray.train.DataConfig
    torch_trainer = TorchTrainer(
        train_loop_per_worker,
        datasets={"train": ray_dataset},
        scaling_config=ScalingConfig(
            num_workers=args.num_workers,
            # use_gpu=True,
        ),
        dataset_config=dataset_config_cls(
            execution_options=options,
        ),
    )
    
    result = torch_trainer.fit()
    # Report the throughput of the last epoch, sum runtime across all workers.
    time_last_epoch = result.metrics["time_final_epoch"]
    time_start_last_epoch = [t[0] for t in time_last_epoch]
    time_end_last_epoch = [t[1] for t in time_last_epoch]
    # print("===> time_start_last_epoch:", time_start_last_epoch, min(time_start_last_epoch))
    # print("===> time_end_last_epoch:", time_end_last_epoch, max(time_end_last_epoch))
    runtime_last_epoch = max(time_end_last_epoch) - min(time_start_last_epoch)
    tput_last_epoch = ray_dataset.count() / runtime_last_epoch
    # print("===> throughput last epoch:", tput_last_epoch, runtime_last_epoch)
    return {BenchmarkMetric.THROUGHPUT.value: tput_last_epoch}

if __name__ == "__main__":
    args = parse_args()
    benchmark_name = f"read_{args.file_type}_repeat{args.repeat_ds}_train_{args.num_workers}workers"
    if args.preserve_order:
        benchmark_name = f"{benchmark_name}_preserve_order"

    benchmark = Benchmark(benchmark_name)

    benchmark.run_fn("cache-none", benchmark_code, args=args)
    # benchmark.run_fn("cache-output", benchmark_code, args=args, cache_output_ds=True)
    # benchmark.run_fn(f"cache-output-read-{args.read_task_cpus}-cpu", benchmark_code, args=args, read_num_cpus=args.read_task_cpus, cache_output_ds=True)
    # benchmark.run_fn("cache-input", benchmark_code, args=args, cache_input_ds=True)
    # benchmark.run_fn("prepartition-ds", benchmark_code, args=args, prepartition_ds=True)
    benchmark.write_result("/tmp/multi_node_train_benchmark.json")
