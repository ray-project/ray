import ray
from ray import train
from ray.train import DataConfig, ScalingConfig, RunConfig, Checkpoint
from ray.train.torch import TorchTrainer
from ray.data.datasource.partitioning import Partitioning
import tempfile
import itertools
import os
import time

from benchmark import Benchmark, BenchmarkMetric
from image_loader_microbenchmark import (
    get_transform,
    crop_and_flip_image,
    decode_image_crop_and_flip,
    center_crop_image,
)

from image_loader_microbenchmark import get_mosaic_dataloader

import torch
import torch.distributed as dist
from torchvision.models import resnet50
from torchvision.transforms.functional import pil_to_tensor
import torch.nn as nn
import torch.optim as optim

from dataset_benchmark_util import (
    get_prop_parquet_paths,
    IMG_S3_ROOT,
    get_mosaic_epoch_size,
    IMAGENET_WNID_TO_ID,
)


# This benchmark does the following:
# 1) Read files (images or parquet) with ray.data
# 2) Apply preprocessing with map()
# 3) Train TorchTrainer on processed data with resnet50 model
# Metrics recorded to the output file are:
# - Runtime of benchmark (s)
# - Final epoch throughput (img/s)
# - Final epoch top-1 accuracy (%)


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
        "--skip-train-model",
        default=False,
        type=bool,
        help="Whether to skip training a model (i.e. only consume data). "
        "Set to True if file_type == 'parquet'.",
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
        "--num-retries",
        default=3,
        type=int,
        help="Number of retries for the Traine before exiting the benchmark.",
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
        "--use-synthetic-data",
        action="store_true",
        default=False,
        help=(
            "Whether to use synthetic Torch data (repeat a "
            "randomly generated batch 1000 times)"
        ),
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

    if not (args.use_torch or args.use_mosaic or args.use_synthetic_data):
        args.use_ray_data = True
    else:
        args.use_ray_data = False

    if args.data_root is None and not args.use_mosaic:
        # use default datasets if data root is not provided
        if args.file_type == "image":
            args.data_root = IMG_S3_ROOT
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
    if args.file_type == "parquet" or args.use_torch or args.use_mosaic:
        # Training model is only supported for images currently.
        # Parquet files do not have labels.
        args.skip_train_model = True
    return args


# Constants and utility methods for image-based benchmarks.
DEFAULT_IMAGE_SIZE = 224


def _get_ray_data_batch_iterator(args, worker_rank):
    if args.split_input:
        it = train.get_dataset_shard(f"train_{worker_rank}")
    else:
        it = train.get_dataset_shard("train")
    return it.iter_torch_batches(
        batch_size=args.batch_size,
        local_shuffle_buffer_size=args.batch_size / 2,
    )


def _get_batch_num_rows(batch):
    if not (args.use_torch or args.use_mosaic):
        return batch["image"].size(dim=0)
    return batch.size(dim=0)


def train_loop_per_worker():
    worker_rank = train.get_context().get_world_rank()
    device = train.torch.get_device()
    world_size = ray.train.get_context().get_world_size()
    local_world_size = ray.train.get_context().get_local_world_size()
    torch_num_workers = args.torch_num_workers or os.cpu_count()
    # Divide by the number of Train workers because each has its own dataloader.
    torch_num_workers //= local_world_size

    # Setup the model
    raw_model = resnet50(weights=None)
    model = train.torch.prepare_model(raw_model)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.SGD(model.parameters(), lr=0.1, momentum=0.9)

    # Get the configured data loading solution.
    batch_iter = None

    if args.use_torch:
        batch_iter = get_torch_data_loader(
            worker_rank=worker_rank,
            batch_size=args.batch_size,
            num_workers=torch_num_workers,
            transform=get_transform(False),
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
        num_physical_nodes = world_size // local_world_size
        batch_iter = get_mosaic_dataloader(
            args.data_root,
            batch_size=args.batch_size,
            num_physical_nodes=num_physical_nodes,
            epoch_size=target_epoch_size,
            num_workers=torch_num_workers,
        )

    all_workers_time_list_across_epochs = []
    validation_accuracy_per_epoch = []
    # Validation loop with non-random cropped dataset
    # is only supported for image dataset.
    run_validation_set = args.use_ray_data and args.file_type == "image"

    # Begin training over the configured number of epochs.
    for epoch in range(args.num_epochs):
        # Ray Data needs to call iter_torch_batches on each epoch.
        if args.use_ray_data:
            batch_iter = _get_ray_data_batch_iterator(args, worker_rank)
            if run_validation_set:
                val_ds = train.get_dataset_shard("val")
                batch_iter_val = val_ds.iter_torch_batches(batch_size=args.batch_size)
        # For synthetic data, we need to create the iterator each epoch.
        elif args.use_synthetic_data:
            # Generate a random batch, and continuously yield the same batch 1000 times.
            NUM_BATCHES_PER_EPOCH = 1000
            sample_batch = {
                "image": torch.rand(
                    (args.batch_size, 3, DEFAULT_IMAGE_SIZE, DEFAULT_IMAGE_SIZE),
                    device=device,
                ),
                "label": torch.randint(
                    0,
                    NUM_BATCHES_PER_EPOCH,
                    (args.batch_size,),
                    device=device,
                ),
            }
            batch_iter = itertools.repeat(sample_batch, NUM_BATCHES_PER_EPOCH)

        print(f"Epoch {epoch+1} of {args.num_epochs}")
        num_rows = 0
        start_t = time.time()
        num_batches = 0.0
        total_loss = 0.0
        for batch_idx, batch in enumerate(batch_iter):
            num_rows += _get_batch_num_rows(batch)

            if not args.skip_train_model:
                # get the inputs; data is a list of [inputs, labels]
                inputs = torch.as_tensor(batch["image"], dtype=torch.float32).to(
                    device=device
                )
                labels = torch.as_tensor(batch["label"], dtype=torch.int64).to(
                    device=device
                )
                # forward + backward + optimize
                outputs = model(inputs)
                loss = criterion(outputs, labels)

                # zero the parameter gradients
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

                num_batches += 1
                total_loss += loss.item()

            # print statistics
            if batch_idx % 2000 == 1999:  # print every 2000 mini-batches
                print(
                    f"[{epoch + 1}, {batch_idx + 1:5d}]"
                    f"loss: {total_loss / 2000:.3f}"
                )
        end_t = time.time()

        epoch_accuracy_val = None
        if run_validation_set:
            print(f"Starting validation set for epoch {epoch+1}")
            num_correct_val = 0
            num_rows_val = 0
            with torch.no_grad():
                for batch_idx, batch in enumerate(batch_iter_val):
                    inputs = torch.as_tensor(batch["image"], dtype=torch.float32).to(
                        device=device
                    )
                    labels = torch.as_tensor(batch["label"], dtype=torch.int64).to(
                        device=device
                    )

                    outputs = model(inputs)
                    loss = criterion(outputs, labels)
                    output_classes = outputs.argmax(dim=1)

                    num_rows_val += len(labels)
                    num_correct_val += (output_classes == labels).sum().item()
            epoch_accuracy_val = num_correct_val / num_rows_val
            validation_accuracy_per_epoch.append(epoch_accuracy_val)

        with tempfile.TemporaryDirectory() as tmpdir:
            torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))
            checkpoint = Checkpoint.from_directory(tmpdir)
            train.report(
                dict(
                    epoch_accuracy=epoch_accuracy_val,
                    loss_avg=(total_loss / num_batches) if num_batches > 0 else 0,
                ),
                checkpoint=checkpoint,
            )

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
            f"Epoch {epoch+1} of {args.num_epochs}, "
            f"tput: {num_rows / (end_t - start_t)}, "
            f"run time: {end_t - start_t}, "
            f"validation accuracy: "
            f"{epoch_accuracy_val * 100 if epoch_accuracy_val else 0:.3f}%"
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

    final_train_report_metrics = {
        **per_epoch_times,
        "num_rows": [tensor.item() for tensor in all_num_rows],
    }

    if run_validation_set:
        all_num_rows_val = [
            torch.zeros((1), dtype=torch.int32, device=device)
            for _ in range(world_size)
        ]
        curr_num_rows_val = torch.tensor(
            [num_rows_val], dtype=torch.int32, device=device
        )
        dist.all_gather(all_num_rows_val, curr_num_rows_val)

        all_num_rows_correct_val = [
            torch.zeros((1), dtype=torch.int32, device=device)
            for _ in range(world_size)
        ]
        curr_num_rows_correct = torch.tensor(
            [num_correct_val], dtype=torch.int32, device=device
        )
        dist.all_gather(all_num_rows_correct_val, curr_num_rows_correct)
        final_train_report_metrics.update(
            {
                "num_rows_val": [tensor.item() for tensor in all_num_rows_val],
                "num_rows_correct_val": [
                    tensor.item() for tensor in all_num_rows_correct_val
                ],
                # Report the validation accuracy of the final epoch
                "epoch_accuracy": validation_accuracy_per_epoch[-1],
            }
        )

    train.report(final_train_report_metrics)


# The input files URLs per training worker.
INPUT_FILES_PER_WORKER = []


def split_input_files_per_worker(args):
    """Set the input files per each training worker."""
    global INPUT_FILES_PER_WORKER
    import numpy as np
    from torchdata.datapipes.iter import IterableWrapper

    data_root_iter = args.data_root
    if isinstance(data_root_iter, str):
        data_root_iter = [data_root_iter]

    file_url_dp = IterableWrapper(data_root_iter).list_files_by_s3()
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
            image = transform(
                pil_to_tensor(image) / 255.0,
            )
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
                # Obtain WNID from filepath, then convert to numerical class ID
                partitioning = Partitioning(
                    "dir",
                    field_names=["class"],
                    base_dir=args.data_root,
                )
                ray_dataset = ray.data.read_images(
                    input_paths,
                    mode="RGB",
                    partitioning=partitioning,
                )

                val_dataset = ray.data.Dataset.copy(ray_dataset)
                # Full random shuffle results in OOM. Instead, use the
                # `ds.iter_batches(local_shuffle_buffer_size=...)`
                # parameter in the training loop.
                # ray_dataset = ray_dataset.random_shuffle()

                def wnid_to_index(row):
                    row["label"] = IMAGENET_WNID_TO_ID[row["class"]]
                    row.pop("class")
                    return row

                ray_dataset = ray_dataset.map(wnid_to_index)
                val_dataset = val_dataset.map(wnid_to_index)
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
                val_dataset = val_dataset.map(center_crop_image)
                ray_datasets_dict["val"] = val_dataset
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
        run_config=RunConfig(
            storage_path="/mnt/cluster_storage",
            failure_config=train.FailureConfig(args.num_retries),
        ),
    )

    result = torch_trainer.fit()
    data_benchmark_metrics = {}

    # Report the average of per-epoch throughput, excluding the first epoch.
    # Unless there is only one epoch, in which case we report the epoch
    # throughput directly.
    start_epoch_tput = 0 if args.num_epochs == 1 else 1
    epoch_tputs = []
    num_rows_per_epoch = sum(result.metrics["num_rows"])
    for i in range(start_epoch_tput, args.num_epochs):
        time_start_epoch_i, time_end_epoch_i = zip(*result.metrics[f"epoch_{i}_times"])
        runtime_epoch_i = max(time_end_epoch_i) - min(time_start_epoch_i)
        tput_epoch_i = num_rows_per_epoch / runtime_epoch_i
        epoch_tputs.append(tput_epoch_i)
    avg_per_epoch_tput = sum(epoch_tputs) / len(epoch_tputs)
    print("Total num rows read per epoch:", num_rows_per_epoch, "images")
    print("Averaged per-epoch throughput:", avg_per_epoch_tput, "img/s")
    data_benchmark_metrics.update(
        {
            BenchmarkMetric.THROUGHPUT: avg_per_epoch_tput,
        }
    )

    # Report the training accuracy of the final epoch.
    if result.metrics.get("num_rows_val") is not None:
        final_epoch_acc = sum(result.metrics["num_rows_correct_val"]) / sum(
            result.metrics["num_rows_val"]
        )
        print(f"Final epoch accuracy: {final_epoch_acc * 100:.3f}%")
        data_benchmark_metrics.update(
            {
                BenchmarkMetric.ACCURACY: final_epoch_acc,
            }
        )
    return data_benchmark_metrics


if __name__ == "__main__":
    args = parse_args()
    data_type = "synthetic" if args.use_synthetic_data else args.file_type
    benchmark_name = (
        f"read_{data_type}_repeat{args.repeat_ds}_train_"
        f"{args.num_workers}workers_{args.target_worker_gb}gb_per_worker"
    )

    if args.preserve_order:
        benchmark_name = f"{benchmark_name}_preserve_order"
    if not args.skip_train_model:
        benchmark_name = f"{benchmark_name}_resnet50"
    if args.cache_input_ds:
        case_name = "cache-input"
    elif args.cache_output_ds:
        case_name = "cache-output"
    else:
        case_name = "cache-none"

    benchmark = Benchmark(benchmark_name)
    benchmark.run_fn(case_name, benchmark_code, args=args)
    benchmark.write_result("/tmp/multi_node_train_benchmark.json")
