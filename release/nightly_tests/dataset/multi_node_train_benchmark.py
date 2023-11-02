import ray
from ray import train
from ray.train import DataConfig, ScalingConfig
from ray.train.torch import TorchTrainer
import os

import torch.distributed as dist

from benchmark import Benchmark, BenchmarkMetric
from image_loader_microbenchmark import (
    get_transform,
    crop_and_flip_image,
    decode_image_crop_and_flip,
)

from image_loader_microbenchmark import get_mosaic_dataloader


import time
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
        default=10,
        type=int,
        help=(
            "Number of GB per worker for selecting a subset "
            "from default dataset. -1 means the whole dataset"
        ),
    )
    parser.add_argument(
        "--batch-size",
        default=32,
        type=int,
        help="Batch size to use.",
    )
    parser.add_argument(
        "--num-epochs",
        # Use 5 epochs and report the avg per-epoch throughput
        # (excluding first epoch in case there is warmup).
        default=5,
        type=int,
        help="Number of epochs to run. The avg per-epoch throughput will be reported.",
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
        help="Whether to pre-split the input dataset instead of using streaming split.",
    )
    parser.add_argument(
        "--cache-input-ds",
        action="store_true",
        default=False,
        help="Whether to cache input dataset (before preprocessing).",
    )
    parser.add_argument(
        "--cache-output-ds",
        action="store_true",
        default=False,
        help="Whether to cache output dataset (after preprocessing).",
    )
    args = parser.parse_args()

    ray.init(
        runtime_env={
            "working_dir": os.path.dirname(__file__),
        }
    )

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


def train_loop_per_worker():
    worker_rank = train.get_context().get_world_rank()
    if args.split_input:
        it = train.get_dataset_shard(f"train_{worker_rank}")
    else:
        it = train.get_dataset_shard("train")
    device = train.torch.get_device()

    batch_iter = None
    if args.use_torch or args.use_mosaic:
        torch_num_workers = args.torch_num_workers or os.cpu_count()
        # Divide by the number of Train workers because each has its own dataloader.
        torch_num_workers //= ray.train.get_context().get_local_world_size()

        if args.use_torch:
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
            batch_iter = get_mosaic_dataloader(
                args.data_root,
                batch_size=args.batch_size,
                num_physical_nodes=num_physical_nodes,
                epoch_size=target_epoch_size,
                num_workers=torch_num_workers,
            )

    world_size = ray.train.get_context().get_world_size()
    all_workers_time_list_across_epochs = []
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
            if not (args.use_torch or args.use_mosaic):
                batch = batch["image"]
            # `batch` should have tensor in `torch.Tensor` format.
            num_rows += batch.size(dim=0)
            if worker_rank == 0 and num_rows >= print_at:
                print(
                    f"Read {num_rows} rows on rank "
                    f"{train.get_context().get_world_rank()}, tput so far: "
                    f"{num_rows / (time.time()  - start_t)}"
                )
                print_at = ((num_rows // print_at_interval) + 1) * print_at_interval
        end_t = time.time()
        # Workaround to report the epoch start/end time from each worker, so that we
        # can aggregate them at the end when calculating throughput.
        all_workers_time_list = [
            torch.zeros((2), dtype=torch.double, device=device)
            for _ in range(world_size)
        ]
        curr_worker_time = torch.tensor(
            [start_t, end_t], dtype=torch.double, device=device
        )
        dist.all_gather(all_workers_time_list, curr_worker_time)
        all_workers_time_list_across_epochs.append(all_workers_time_list)

        print(
            f"Epoch {i+1} of {args.num_epochs}, tput: {num_rows / (end_t - start_t)}, "
            f"run time: {end_t - start_t}"
        )
    # Similar reporting for aggregating number of rows across workers
    all_num_rows = [
        torch.zeros((1), dtype=torch.int32, device=device) for _ in range(world_size)
    ]
    curr_num_rows = torch.tensor([num_rows], dtype=torch.int32, device=device)
    dist.all_gather(all_num_rows, curr_num_rows)

    per_epoch_times = {
        f"epoch_{i}_times": [
            tensor.tolist() for tensor in all_workers_time_list_across_epochs[i]
        ]
        for i in range(args.num_epochs)
    }
    train.report(
        {
            **per_epoch_times,
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
):
    cache_input_ds = args.cache_input_ds
    cache_output_ds = args.cache_output_ds
    assert (
        sum([cache_output_ds, cache_input_ds]) <= 1
    ), "Can only test one caching variant at a time"

    if args.use_torch or args.split_input:
        split_input_files_per_worker(args)

    ray_datasets_dict = {}
    if not (args.use_mosaic or args.use_torch):
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
                )
            elif args.file_type == "parquet":
                ray_dataset = ray.data.read_parquet(
                    args.data_root,
                )
            else:
                raise Exception(f"Unknown file type {args.file_type}")

            if cache_input_ds:
                ray_dataset = ray_dataset.materialize()

            # 2) Preprocess data by applying transformation with map/map_batches()
            if args.file_type == "image":
                ray_dataset = ray_dataset.map(crop_and_flip_image)
            elif args.file_type == "parquet":
                ray_dataset = ray_dataset.map(decode_image_crop_and_flip)
            if cache_output_ds:
                ray_dataset = ray_dataset.materialize()
            ray_datasets_dict[ds_name] = ray_dataset

    # 3) Train TorchTrainer on processed data
    options = DataConfig.default_ingest_options()
    options.preserve_order = args.preserve_order

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

    result = torch_trainer.fit()

    # Report the average of per-epoch throughput, excluding the first epoch.
    epoch_tputs = []
    num_rows_per_epoch = sum(result.metrics["num_rows"])
    for i in range(1, args.num_epochs):
        time_start_epoch_i, time_end_epoch_i = zip(*result.metrics[f"epoch_{i}_times"])
        runtime_epoch_i = max(time_end_epoch_i) - min(time_start_epoch_i)
        tput_epoch_i = num_rows_per_epoch / runtime_epoch_i
        epoch_tputs.append(tput_epoch_i)
    avg_per_epoch_tput = sum(epoch_tputs) / len(epoch_tputs)
    print("Total num rows read per epoch:", num_rows_per_epoch, "images")
    print("Averaged per-epoch throughput:", avg_per_epoch_tput, "img/s")
    return {
        BenchmarkMetric.THROUGHPUT.value: avg_per_epoch_tput,
    }


if __name__ == "__main__":
    args = parse_args()
    benchmark_name = (
        f"read_{args.file_type}_repeat{args.repeat_ds}_train_"
        f"{args.num_workers}workers_{args.target_worker_gb}gb_per_worker"
    )

    if args.preserve_order:
        benchmark_name = f"{benchmark_name}_preserve_order"
    if args.cache_input_ds:
        case_name = "cache-input"
    elif args.cache_output_ds:
        case_name = "cache-output"
    else:
        case_name = "cache-none"

    benchmark = Benchmark(benchmark_name)
    benchmark.run_fn(case_name, benchmark_code, args=args)
    benchmark.write_result("/tmp/multi_node_train_benchmark.json")
