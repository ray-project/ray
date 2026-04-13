"""Benchmark: local buffer shuffle vs map_batches shuffle.

Measures steady-state training throughput (rows/sec) with Ray Data's local
buffer shuffle and map_batches distributed shuffle at different buffer sizes.

Setup:
    - Dataset: ray.data.range_tensor(81_920_000, shape=(512,))  (~4KB/row)
    - Model: Linear(512, 10) -- trivial, measures data pipeline not model
    - Batch size: 4096 per worker, 4 GPU workers
    - 200 steps per run, first 100 warmup (excluded from steady throughput)
"""

import json
import os
import time

import numpy as np
import pyarrow as pa
import ray
import ray.data
import ray.train
import ray.train.torch
import torch

NUM_ROWS = 81_920_000
TENSOR_DIM = 512
ROW_BYTES = TENSOR_DIM * 8  # 4096 bytes per row
BATCH_SIZE = 4096
MAX_STEPS = 200
WARMUP_STEPS = 100


def random_shuffle(batch: pa.Table) -> pa.Table:
    """Randomly shuffle rows within a PyArrow batch."""
    indices = np.random.permutation(len(batch))
    return batch.take(indices)


def train_fn(config):
    """Training loop that measures steady-state throughput."""
    warmup_steps = config["warmup_steps"]
    max_steps = config["max_steps"]
    metrics_path = config["metrics_path"]
    buffer_size = config["buffer_size"]

    device = ray.train.torch.get_device()
    model = ray.train.torch.prepare_model(torch.nn.Linear(TENSOR_DIM, 10))
    loss_fn = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3)

    ds_iter = ray.train.get_dataset_shard("train")
    iter_kwargs = dict(
        batch_size=BATCH_SIZE,
        prefetch_batches=4,
        drop_last=True,
    )
    if buffer_size > 0 and config.get("mode") == "local":
        iter_kwargs["local_shuffle_buffer_size"] = buffer_size
    dataloader = ds_iter.iter_torch_batches(**iter_kwargs)

    world_size = ray.train.get_context().get_world_size()
    global_batch_size = BATCH_SIZE * world_size

    total_rows = 0
    steady_start = None
    steady_rows = 0
    step = 0

    t0 = time.perf_counter()
    for batch in dataloader:
        data = batch["data"].float().to(device)
        labels = data[:, 0].long() % 10

        optimizer.zero_grad()
        loss = loss_fn(model(data), labels)
        loss.backward()
        optimizer.step()

        total_rows += global_batch_size
        step += 1

        if step > warmup_steps:
            if steady_start is None:
                steady_start = time.perf_counter()
            steady_rows += global_batch_size

        if max_steps > 0 and step >= max_steps:
            break

    elapsed = time.perf_counter() - t0
    steady_elapsed = (time.perf_counter() - steady_start) if steady_start else 0

    metrics = {
        "throughput": total_rows / elapsed if elapsed > 0 else 0,
        "steady_throughput": steady_rows / steady_elapsed if steady_elapsed > 0 else 0,
        "elapsed": elapsed,
        "steps": step,
    }
    ray.train.report(metrics)

    if ray.train.get_context().get_world_rank() == 0:
        with open(metrics_path, "w") as f:
            json.dump(metrics, f)


def run_once(buffer_rows, num_workers, mode="none"):
    """Run one benchmark: create dataset, optionally add shuffle, train."""
    ds = ray.data.range_tensor(NUM_ROWS, shape=(TENSOR_DIM,))

    if mode == "map_batches" and buffer_rows > 0:
        shuffle_mem = int(buffer_rows * ROW_BYTES)
        ds = ds.map_batches(
            random_shuffle,
            batch_size=buffer_rows,
            batch_format="pyarrow",
            num_cpus=0,
            memory=shuffle_mem,
        )

    metrics_path = f"/mnt/cluster_storage/bench_{os.urandom(4).hex()}.json"

    trainer = ray.train.torch.TorchTrainer(
        train_loop_per_worker=train_fn,
        train_loop_config={
            "warmup_steps": WARMUP_STEPS,
            "max_steps": MAX_STEPS,
            "metrics_path": metrics_path,
            "buffer_size": buffer_rows,
            "mode": mode,
        },
        scaling_config=ray.train.ScalingConfig(
            num_workers=num_workers,
            use_gpu=True,
        ),
        datasets={"train": ds},
    )
    trainer.fit()

    metrics = {}
    for _ in range(10):
        if os.path.exists(metrics_path):
            try:
                with open(metrics_path) as f:
                    metrics = json.load(f)
                os.remove(metrics_path)
                break
            except Exception:
                pass
        time.sleep(1)
    return metrics
