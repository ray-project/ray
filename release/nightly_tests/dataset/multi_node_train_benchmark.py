import ray
from ray import train
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.data.datasource import file_based_datasource
import os

import torch.distributed as dist
import numpy as np
import math

from benchmark import Benchmark, BenchmarkMetric

from PIL import Image
from image_loader_microbenchmark import get_mosaic_dataloader


import time
import torchvision
import torch

from dataset_benchmark_util import (
    get_prop_parquet_paths,
    get_prop_raw_image_paths,
    get_mosaic_epoch_size,
)


# This benchmark does the following:
# 1) Read files (images or parquet) with ray.data
# 2) Apply preprocessing with map()
# 3) Train TorchTrainer on processed data
# Metrics recorded to the output file are:
# - ray.torchtrainer.fit: Throughput of the final epoch in
#   TorchTrainer.fit() (step 3 above)


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
        "--target-worker-gb",
        default=4,
        type=int,
        help="Number of GB per worker for selecting a subset from default dataset. -1 means the whole dataset",
    )
    parser.add_argument(
        "--read-task-cpus",
        default=1,
        type=float,
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
        "--use-torch",
        action="store_true",
        default=False,
        help="Whether to use PyTorch DataLoader.",
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
        "--split-input",
        action="store_true",
        default=False,
        help="Whether to split input dataset.",
    )
    args = parser.parse_args()

    if args.data_root is None and not args.use_mosaic:
        # use default datasets if data root is not provided
        if args.file_type == "image":
            args.data_root = get_prop_raw_image_paths(
                num_workers=args.num_workers, target_worker_gb=args.target_worker_gb
            )
        elif args.file_type == "parquet":
            args.data_root = get_prop_parquet_paths(
                num_workers=args.num_workers, target_worker_gb=args.target_worker_gb
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


def decode_image_crop_and_flip(row):
    transform = get_transform(False)
    row["image"] = Image.frombytes("RGB", (row["height"], row["width"]), row["image"])
    del row["width"]
    del row["height"]
    # Convert back np to avoid storing a np.object array.
    row["image"] = np.array(transform(row["image"]))
    return row


def train_loop_per_worker():
    worker_rank = train.get_context().get_world_rank()
    if args.split_input:
        it = train.get_dataset_shard(f"train_{worker_rank}")
    else:
        it = train.get_dataset_shard("train")
    device = train.torch.get_device()

    batch_iter = None
    if args.use_torch:
        torch_num_workers = args.torch_num_workers
        if torch_num_workers is None:
            torch_num_workers = 256
        batch_iter = get_torch_data_loader(
            worker_rank=worker_rank,
            batch_size=args.batch_size,
            num_workers=torch_num_workers,
            transform=get_transform(True),
        )
    elif args.use_mosaic:
        target_epoch_size = get_mosaic_epoch_size(
            args.num_workers, target_worker_gb=args.target_worker_gb
        )
        print(
            "Epoch size:",
            target_epoch_size if target_epoch_size is not None else "all",
            "images",
        )

        num_physical_nodes = (
            ray.train.get_context().get_world_size()
            // ray.train.get_context().get_local_world_size()
        )
        torch_num_workers = args.torch_num_workers
        if torch_num_workers is None:
            torch_num_workers = os.cpu_count()
        # Divide by the number of Train workers because each has its own dataloader.
        torch_num_workers //= ray.train.get_context().get_local_world_size()

        batch_iter = get_mosaic_dataloader(
            args.data_root,
            batch_size=args.batch_size,
            num_physical_nodes=num_physical_nodes,
            epoch_size=target_epoch_size,
            num_workers=torch_num_workers,
        )

    for i in range(args.num_epochs):
        print(f"Epoch {i+1} of {args.num_epochs}")
        num_rows = 0
        start_t = time.time()

        # Ray Data needs to call iter_torch_batches on each epoch.
        if isinstance(it, ray.data.iterator.DataIterator):
            batch_iter = it.iter_torch_batches(
                batch_size=args.batch_size,
            )

        print_at_interval = 1000
        print_at = print_at_interval
        for batch in batch_iter:
            # `batch` should have tensor in `torch.Tensor` format.
            if args.use_torch:
                num_rows += batch.size(dim=0)
            elif args.use_mosaic:
                num_rows += args.batch_size
            else:
                num_rows += batch["image"].size(dim=0)
            if num_rows >= print_at:
                print(
                    f"Read {num_rows} rows on rank {train.get_context().get_world_rank()}, tput so far: {num_rows / (time.time()  - start_t)}"
                )
                print_at = ((num_rows // print_at_interval) + 1) * print_at_interval
        end_t = time.time()
        print(
            f"Epoch {i+1} of {args.num_epochs}, tput: {num_rows / (end_t - start_t)}, run time: {end_t - start_t}"
        )

    # Workaround to report the final epoch time from each worker, so that we
    # can sum up the times at the end when calculating throughput.
    # See: https://github.com/ray-project/ray/issues/39277
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


# The input files URLs per training worker.
INPUT_FILES_PER_WORKER = []


def split_input_files_per_worker(args):
    """Set the input files per each training worker."""
    global INPUT_FILES_PER_WORKER
    import numpy as np
    from torchdata.datapipes.iter import IterableWrapper

    file_url_dp = IterableWrapper(args.data_root).list_files_by_s3()
    all_files = list(file_url_dp)
    INPUT_FILES_PER_WORKER = [
        f.tolist() for f in np.array_split(all_files, args.num_workers)
    ]


def get_torch_data_loader(worker_rank, batch_size, num_workers, transform=None):
    """Get PyTorch DataLoader for the specified training worker.

    The input files are split across all workers, and this PyTorch DataLoader
    would only read the portion of files for itself.
    """
    import os
    import numpy as np
    from torchdata.datapipes.iter import IterableWrapper, S3FileLoader

    # NOTE: these two variables need to be set to read from S3 successfully.
    os.environ["S3_VERIFY_SSL"] = "0"
    os.environ["AWS_REGION"] = "us-west-2"

    def load_image(inputs):
        import io
        from PIL import Image

        url, fd = inputs
        data = fd.file_obj.read()
        image = Image.open(io.BytesIO(data))
        image = image.convert("RGB")
        if transform is not None:
            image = transform(image)
        return image

    class FileURLDataset:
        """The PyTorch Dataset to split input files URLs among workers."""

        def __init__(self, file_urls):
            self._file_urls = file_urls

        def __iter__(self):
            worker_info = torch.utils.data.get_worker_info()
            assert worker_info is not None

            torch_worker_id = worker_info.id
            return iter(self._file_urls[torch_worker_id])

    file_urls = INPUT_FILES_PER_WORKER[worker_rank]
    file_urls = [f.tolist() for f in np.array_split(file_urls, num_workers)]
    file_url_dp = IterableWrapper(FileURLDataset(file_urls))
    file_dp = S3FileLoader(file_url_dp)
    image_dp = file_dp.map(load_image)

    # NOTE: the separate implementation for using fsspec.
    # Comment out by default. Leave it here as reference.
    #
    # subdir_url_dp = IterableWrapper([root_dir]).list_files_by_fsspec()
    # file_url_dp = subdir_url_dp.list_files_by_fsspec()
    # file_dp = file_url_dp.open_files_by_fsspec(mode="rb")
    # image_dp = file_dp.map(load_image)

    data_loader = torch.utils.data.DataLoader(
        image_dp,
        batch_size=batch_size,
        num_workers=num_workers,
    )
    return data_loader


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

    if args.use_torch or args.split_input:
        split_input_files_per_worker(args)

    ray_datasets_dict = {}
    if not args.use_mosaic:
        # Only create one dataset if `args.split_input` is True.
        # Otherwise, create N datasets for N training workers,
        # each dataset reads the corresponding portion of input data.
        num_datasets = 1
        if args.split_input:
            num_datasets = args.num_workers

        for i in range(num_datasets):
            if args.split_input:
                input_paths = INPUT_FILES_PER_WORKER[i]
                ds_name = f"train_{i}"
            else:
                input_paths = args.data_root
                ds_name = "train"

            # 1) Read in data with read_images() / read_parquet()
            if args.file_type == "image":
                ray_dataset = ray.data.read_images(
                    input_paths,
                    mode="RGB",
                    ray_remote_args={"num_cpus": args.read_task_cpus},
                )
            elif args.file_type == "parquet":
                ray_dataset = ray.data.read_parquet(
                    args.data_root,
                    ray_remote_args={"num_cpus": args.read_task_cpus},
                )
            else:
                raise Exception(f"Unknown file type {args.file_type}")

            if cache_input_ds:
                ray_dataset = ray_dataset.materialize()

            # 2) Preprocess data by applying transformation with map/map_batches()
            if args.file_type == "image":
                ray_dataset = ray_dataset.map(
                    crop_and_flip_image, num_cpus=args.read_task_cpus
                )
            elif args.file_type == "parquet":
                ray_dataset = ray_dataset.map(decode_image_crop_and_flip)
            if cache_output_ds:
                ray_dataset = ray_dataset.materialize()
            ray_datasets_dict[ds_name] = ray_dataset

    # 3) Train TorchTrainer on processed data
    options = DataConfig.default_ingest_options()
    options.preserve_order = args.preserve_order

    if args.num_workers == 1 or args.use_gpu:
        torch_trainer = TorchTrainer(
            train_loop_per_worker,
            datasets=ray_datasets_dict,
            scaling_config=ScalingConfig(
                num_workers=args.num_workers,
                use_gpu=args.use_gpu,
            ),
            dataset_config=ray.train.DataConfig(
                datasets_to_split=[] if args.split_input else "all",
                execution_options=options,
            ),
        )
    else:
        torch_trainer = TorchTrainer(
            train_loop_per_worker,
            datasets=ray_datasets_dict,
            # In the multi-node case without GPUs, we use a SPREAD placement strategy
            # to ensure that tasks are spread across nodes. We reserve one worker
            # for the driver.
            scaling_config=ScalingConfig(
                num_workers=args.num_workers - 1,
                use_gpu=args.use_gpu,
                placement_strategy="STRICT_SPREAD",
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
    # Workaround for FileBasedDatasource parallel read issue when reading many sources.
    file_based_datasource.FILE_SIZE_FETCH_PARALLELIZATION_THRESHOLD = 1000
    args = parse_args()
    benchmark_name = f"read_{args.file_type}_repeat{args.repeat_ds}_train_{args.num_workers}workers_{args.target_worker_gb}gb_per_worker"
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
