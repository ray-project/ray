from collections import OrderedDict
import argparse
import os
import json
import ray
import time
import timeit

import torch.optim as optim
import numpy as np
import torch
import horovod.torch as hvd
from horovod.ray import RayExecutor

from ray_shuffling_data_loader.data_generation import DATA_SPEC
from ray_shuffling_data_loader.embedding_model import MyModel, annotation, huber_loss
from ray.data.dataset_pipeline import DatasetPipeline

# Training settings
parser = argparse.ArgumentParser(description="Dataset ingestion Example")
parser.add_argument(
    "--batch-size",
    type=int,
    default=250000,
    metavar="N",
    help="input batch size for training (default: 64)",
)
parser.add_argument(
    "--epochs",
    type=int,
    default=10,
    metavar="N",
    help="number of epochs to train (default: 10)",
)
parser.add_argument("--debug", action="store_true", default=False, help="disables hvd")
parser.add_argument(
    "--seed", type=int, default=42, metavar="S", help="random seed (default: 42)"
)
parser.add_argument(
    "--log-interval",
    type=int,
    default=10,
    metavar="N",
    help=("how many batches to wait before logging training " "status"),
)
parser.add_argument("--num-workers", type=int, default=16)
parser.add_argument("--mock-train-step-time", type=float, default=1.0)
parser.add_argument("--num-files", type=int, default=30)
parser.add_argument("--num-windows", type=int, default=1)

SIZE_50_G = 30  # 49.17GB
SIZE_100_G = 62  # 101.62GB
SIZE_500_G = 305  # 499.93GB
SIZE_1500_G = 915


def construct_optimizers(model):
    sparse_params = []
    dense_params = []
    for k, v in model.named_parameters():
        if "input.embeddings.embeddings" in k:
            sparse_params.append((k, v))
        else:
            dense_params.append((k, v))

    optimizers = []
    if len(dense_params) > 0:
        opt = optim.Adam([v for _, v in dense_params], lr=0.001)
        opt = hvd.DistributedOptimizer(opt, dense_params)
        optimizers.append(opt)
    if len(sparse_params) > 0:
        opt = optim.SparseAdam([v for _, v in sparse_params], lr=0.001)
        opt = hvd.DistributedOptimizer(opt, sparse_params)
        optimizers.append(opt)

    if hvd.rank() == 0:
        print(optimizers)

    return optimizers


def train_main(args, splits):
    # Horovod: initialize library.
    hvd.init()
    torch.manual_seed(args.seed)

    if torch.cuda.is_available():
        # Horovod: pin GPU to local rank.
        torch.cuda.set_device(hvd.local_rank())
        torch.cuda.manual_seed(args.seed)

    # Horovod: limit # of CPU threads to be used per worker.
    torch.set_num_threads(1)
    rank = hvd.rank()

    model = MyModel(annotation, use_bn=False)
    # By default, Adasum doesn"t need scaling up learning rate.
    if torch.cuda.is_available():
        # Move model to GPU.
        model.cuda()

    optimizers = construct_optimizers(model)
    loss_function = huber_loss
    # Horovod: broadcast parameters & optimizer state.
    hvd.broadcast_parameters(model.state_dict(), root_rank=0)
    for opt in optimizers:
        hvd.broadcast_optimizer_state(opt, root_rank=0)

    def _train(epoch, train_dataset):
        model.train()
        # Horovod: set epoch to sampler for shuffling.
        # train_dataset.set_epoch(epoch)
        start_epoch = timeit.default_timer()
        last_batch_time = start_epoch
        batch_wait_times = []
        for batch_idx, (data, target) in enumerate(train_dataset):
            batch_wait_times.append(timeit.default_timer() - last_batch_time)
            if torch.cuda.is_available():
                data = data.cuda()
                target = target.cuda()
            for opt in optimizers:
                opt.zero_grad()
            batch = OrderedDict()
            batch["embeddings"] = OrderedDict()
            batch["one_hot"] = OrderedDict()
            for i, name in enumerate(annotation["embeddings"]):
                batch["embeddings"][name] = data[:, i : i + 1]
            batch["one_hot"]["hot0"] = data[:, -2:-1]
            batch["one_hot"]["hot1"] = data[:, -1:]

            batch_pred = model(batch)

            if batch_idx % args.log_interval == 0:
                print(
                    f"Processing batch {batch_idx} in epoch {epoch} on worker "
                    f"{rank}."
                )
            time.sleep(args.mock_train_step_time)
            loss = loss_function(batch_pred, target, delta=60)
            loss.mean().backward()
            for opt in optimizers:
                opt.step()

            last_batch_time = timeit.default_timer()
        epoch_duration = timeit.default_timer() - start_epoch
        avg_batch_wait_time = np.mean(batch_wait_times)
        std_batch_wait_time = np.std(batch_wait_times)
        max_batch_wait_time = np.max(batch_wait_times)
        min_batch_wait_time = np.min(batch_wait_times)
        print(
            f"\nEpoch {epoch}, worker {rank} stats over "
            f"{len(batch_wait_times)} steps: {epoch_duration:.3f}"
        )
        print(
            f"Mean batch wait time: {avg_batch_wait_time:.3f}s +- "
            f"{std_batch_wait_time}"
        )
        print(f"Max batch wait time: {max_batch_wait_time:.3f}s")
        print(f"Min batch wait time: {min_batch_wait_time:.3f}s")
        return batch_wait_times

    print(f"Starting training on worker {rank}.")
    batch_wait_times = []
    for epoch, split_ds in enumerate(splits[rank].iter_epochs()):
        train_dataset = create_torch_iterator(split_ds, args.batch_size, rank)
        new_batch_times = _train(epoch, train_dataset)
        new_batch_times.pop(0)
        batch_wait_times.extend(new_batch_times)
    print(f"Done training on worker {rank}.")
    avg_batch_wait_time = np.mean(batch_wait_times)
    std_batch_wait_time = np.std(batch_wait_times)
    max_batch_wait_time = np.max(batch_wait_times)
    min_batch_wait_time = np.min(batch_wait_times)
    print(f"\nWorker {rank} training stats over {args.epochs} epochs:")
    print(
        f"Mean batch wait time: {avg_batch_wait_time:.3f}s +- " f"{std_batch_wait_time}"
    )
    print(f"Max batch wait time: {max_batch_wait_time:.3f}s")
    print(f"Min batch wait time: {min_batch_wait_time:.3f}s")


######################################################

numpy_to_torch_dtype = {
    np.bool: torch.bool,
    np.uint8: torch.uint8,
    np.int8: torch.int8,
    np.int16: torch.int16,
    np.int32: torch.int32,
    np.int64: torch.int64,
    np.float16: torch.float16,
    np.float32: torch.float32,
    np.float64: torch.float64,
    np.complex64: torch.complex64,
    np.complex128: torch.complex128,
}


def create_torch_iterator(split, batch_size, rank=None):
    print(
        f"Creating Torch shuffling dataset for worker {rank} with "
        f"{batch_size} batch size."
    )
    feature_columns = list(DATA_SPEC.keys())
    feature_types = [numpy_to_torch_dtype[dtype] for _, _, dtype in DATA_SPEC.values()]
    label_column = feature_columns.pop()
    label_type = feature_types.pop()

    torch_iterator = split.to_torch(
        label_column=label_column,
        feature_columns=feature_columns,
        label_column_dtype=label_type,
        feature_column_dtypes=feature_types[0],
        batch_size=batch_size,
        # prefetch_blocks: int = 0,
        # drop_last: bool = False
    )
    return torch_iterator


def create_dataset(files, num_workers=4, epochs=50, num_windows=1):
    if num_windows > 1:
        num_rows = ray.data.read_parquet(
            files, _spread_resource_prefix="node:"
        ).count()  # This should only read Parquet metadata.
        file_splits = np.array_split(files, num_windows)

        class Windower:
            def __init__(self):
                self.i = 0
                self.iterations = epochs * num_windows

            def __iter__(self):
                return self

            def __next__(self):
                if self.i >= self.iterations:
                    raise StopIteration()
                split = file_splits[self.i % num_windows]
                self.i += 1
                return lambda: ray.data.read_parquet(
                    list(split), _spread_resource_prefix="node:"
                )

        pipe = DatasetPipeline.from_iterable(Windower())
        split_indices = [
            i * num_rows // num_windows // num_workers for i in range(1, num_workers)
        ]
        pipe = pipe.random_shuffle_each_window(_spread_resource_prefix="node:")
        pipe_shards = pipe.split_at_indices(split_indices)
    else:
        ds = ray.data.read_parquet(files, _spread_resource_prefix="node:")
        pipe = ds.repeat(epochs)
        pipe = pipe.random_shuffle_each_window(_spread_resource_prefix="node:")
        pipe_shards = pipe.split(num_workers, equal=True)
    return pipe_shards


@ray.remote
def consume(split, rank=None, batch_size=None):
    torch_iterator = create_torch_iterator(split, batch_size=batch_size, rank=rank)
    for i, (x, y) in enumerate(torch_iterator):
        if i % 10 == 0:
            print(i)
    return


if __name__ == "__main__":
    args = parser.parse_args()
    import ray

    print("Connecting to Ray cluster...")
    ray.init(address="auto")

    num = args.num_files

    files = [
        f"s3://shuffling-data-loader-benchmarks/data/r10_000_000_000-f1000"
        f"/input_data_{i}.parquet.snappy"
        for i in range(args.num_files)
    ]

    start = time.time()

    splits = create_dataset(
        files,
        num_workers=args.num_workers,
        epochs=args.epochs,
        num_windows=args.num_windows,
    )

    if args.debug:
        tasks = [
            consume.options(num_gpus=1, num_cpus=0).remote(
                split, rank=idx, batch_size=args.batch_size
            )
            for idx, split in enumerate(splits)
        ]
        ray.get(tasks)
    else:
        print("Create Ray executor")
        settings = RayExecutor.create_settings(timeout_s=30)
        executor = RayExecutor(settings, num_workers=args.num_workers, use_gpu=True)
        executor.start()
        executor.run(train_main, args=[args, splits])
        executor.shutdown()

    delta = time.time() - start
    print(f"success! total time {delta}")
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"ingest_time": delta, "success": 1}))
