#!/usr/bin/env python3

import logging
import os
import time
from datetime import datetime

from benchmark import Benchmark
from checkpoint_benchmark import (
    clean_up_output_files,
    run_checkpoints_benchmark,
)
from ray.anyscale.data.checkpoint import CheckpointBackend, CheckpointConfig

if __name__ != "__main__":
    raise RuntimeError("This script should be run as the main entry point")

run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

DATA_DIR = f"/mnt/cluster_storage/checkpoint_benchmark/data/{run_id}"
CHECKPOINT_DIR_DISK = f"/mnt/cluster_storage/checkpoint_benchmark/checkpoints/{run_id}"
S3_BUCKET = os.environ["ANYSCALE_ARTIFACT_STORAGE"]
CHECKPOINT_DIR_S3 = f"{S3_BUCKET}/ray-data-tests/checkpoint_benchmark/{run_id}"
INFERENCE_CONCURRENCY = 8
TRANSFORM_SLEEP_S = 0.001

logger = logging.Logger(__name__)

benchmark = Benchmark()

for workload_type in ["small", "large"]:
    if workload_type == "small":
        # 10k rows and 10MB per row.
        NUM_ROWS = 10_000
        SIZE_BYTES_PER_ROW = 10_000_000
        INFERENCE_BATCH_SIZE = 10
        INFERENCE_SLEEP_S = 0.04
        BACKENDS = ["None", "DISK_BATCH", "S3_BATCH", "DISK_ROW", "S3_ROW"]
    else:
        # 1M rows and 100KB per row.
        NUM_ROWS = 1_000_000
        SIZE_BYTES_PER_ROW = 100_000
        INFERENCE_BATCH_SIZE = 100
        INFERENCE_SLEEP_S = 0.2
        # Skip row-based backends because they are too slow.
        BACKENDS = ["None", "DISK_BATCH", "S3_BATCH"]

    for backend in BACKENDS:
        logger.info(f"Running checkpoint benchmark with backend {backend}")
        start_time = time.time()
        data_dir = f"{DATA_DIR}/{backend}"

        if backend == "None":
            checkpoint_config = CheckpointConfig(enabled=False)
        elif backend == "DISK_BATCH":
            checkpoint_config = CheckpointConfig(
                enabled=True,
                backend=CheckpointBackend.DISK_BATCH,
                id_col="id",
                output_path=f"{CHECKPOINT_DIR_DISK}/{backend}",
            )
        elif backend == "DISK_ROW":
            checkpoint_config = CheckpointConfig(
                enabled=True,
                backend=CheckpointBackend.DISK_ROW,
                id_col="id",
                output_path=f"{CHECKPOINT_DIR_DISK}/{backend}",
            )
        elif backend == "S3_BATCH":
            checkpoint_config = CheckpointConfig(
                enabled=True,
                backend=CheckpointBackend.S3_BATCH,
                id_col="id",
                output_path=f"{CHECKPOINT_DIR_S3}/{backend}",
            )
        elif backend == "S3_ROW":
            checkpoint_config = CheckpointConfig(
                enabled=True,
                backend=CheckpointBackend.S3_ROW,
                id_col="id",
                output_path=f"{CHECKPOINT_DIR_S3}/{backend}",
            )
        else:
            raise ValueError(f"Unknown checkpoint backend: {backend}")

        try:
            run_checkpoints_benchmark(
                benchmark,
                checkpoint_config=checkpoint_config,
                num_rows=NUM_ROWS,
                size_bytes_per_row=SIZE_BYTES_PER_ROW,
                transform_sleep_s=TRANSFORM_SLEEP_S,
                inference_sleep_s=INFERENCE_SLEEP_S,
                inference_batch_size=INFERENCE_BATCH_SIZE,
                inference_concurrency=INFERENCE_CONCURRENCY,
                data_output_path=data_dir,
                benchmark_name=workload_type,
            )
        finally:
            clean_up_output_files(
                checkpoint_config=checkpoint_config,
                data_output_path=data_dir,
            )
        end_time = time.time()
        logger.info(
            f"Checkpoint benchmark with backend {backend} finished "
            f"in {end_time - start_time:.2f} seconds"
        )

benchmark.write_result()
