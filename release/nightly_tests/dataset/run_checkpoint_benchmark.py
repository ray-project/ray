#!/usr/bin/env python3

import os
import sys
from datetime import datetime

from benchmark import Benchmark
from checkpoint_benchmark import (
    clean_up_output_files,
    run_checkpoints_benchmark,
)

from ray.data.checkpoint import CheckpointConfig

if __name__ != "__main__":
    raise RuntimeError("This script should be run as the main entry point")

run_id = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

DATA_DIR = f"/mnt/cluster_storage/checkpoint_benchmark/data/{run_id}"
CHECKPOINT_DIR_FILE_STORAGE = (
    f"/mnt/cluster_storage/checkpoint_benchmark/checkpoints/{run_id}"
)
S3_BUCKET = os.environ["ANYSCALE_ARTIFACT_STORAGE"]
CHECKPOINT_DIR_CLOUD_OBJECT_STORAGE = (
    f"{S3_BUCKET}/ray-data-tests/checkpoint_benchmark/{run_id}"
)
TRANSFORM_SLEEP_S = 0.1
BACKENDS = [
    None,
    "FILE_STORAGE",
    "CLOUD_OBJECT_STORAGE",
]


benchmark = Benchmark()

assert (
    len(sys.argv) == 3
), "Usage: python run_checkpoint_benchmark.py [small|large] [default_id|generated_id_column]"
scale = sys.argv[1]
assert scale in ["small", "large"], scale
id_column_option = sys.argv[2]
assert id_column_option in ["default_id", "generated_id_column"], id_column_option
if id_column_option == "generated_id_column":
    id_column = None
    generated_id_column = "generated_id"
else:
    assert id_column_option == "default_id", id_column_option
    id_column = "id"
    generated_id_column = None

INPUT_DATA_PREFIX = (
    "s3://ray-benchmark-data-internal-us-west-2/ray-data/checkpoint-benchmark"
)

if scale == "small":
    # This dataset contains 10M rows, 5KB per row.
    INPUT_DATA_PATH = f"{INPUT_DATA_PREFIX}/10M-rows/"
    NUM_ROWS = 10_000_000
    INFERENCE_CONCURRENCY = 16
    INFERENCE_BATCH_SIZE = 1000
    INFERENCE_SLEEP_S = 0.1
    NUM_OUTPUT_FILES = 20
else:
    # This dataset contains 3B rows, 500B per row.
    INPUT_DATA_PATH = f"{INPUT_DATA_PREFIX}/3B-rows/"
    NUM_ROWS = 3_000_000_000
    INFERENCE_CONCURRENCY = 200
    INFERENCE_BATCH_SIZE = 10000
    INFERENCE_SLEEP_S = 0.2
    NUM_OUTPUT_FILES = 1500

for backend in BACKENDS:
    benchmark_name = (
        f"checkpoint_benchmark:scale={scale},"
        f"backend={backend},"
        f"id_column={id_column_option}"
    )
    path_suffix = f"{scale}-{backend}"
    data_dir = f"{DATA_DIR}/{path_suffix}"

    if backend is None:
        checkpoint_config = None
    elif backend == "FILE_STORAGE":
        checkpoint_config = CheckpointConfig(
            id_column=id_column,
            generated_id_column=generated_id_column,
            checkpoint_path=f"{CHECKPOINT_DIR_FILE_STORAGE}/{path_suffix}",
        )
    elif backend == "CLOUD_OBJECT_STORAGE":
        checkpoint_config = CheckpointConfig(
            id_column=id_column,
            generated_id_column=generated_id_column,
            checkpoint_path=f"{CHECKPOINT_DIR_CLOUD_OBJECT_STORAGE}/{path_suffix}",
        )
    else:
        raise ValueError(f"Unknown checkpoint backend: {backend}")

    try:
        run_checkpoints_benchmark(
            benchmark,
            checkpoint_config=checkpoint_config,
            input_data_path=INPUT_DATA_PATH,
            transform_sleep_s=TRANSFORM_SLEEP_S,
            inference_sleep_s=INFERENCE_SLEEP_S,
            inference_batch_size=INFERENCE_BATCH_SIZE,
            inference_concurrency=INFERENCE_CONCURRENCY,
            data_output_path=data_dir,
            num_output_files=NUM_OUTPUT_FILES,
            benchmark_name=benchmark_name,
            num_rows=NUM_ROWS,
        )
    finally:
        clean_up_output_files(
            checkpoint_config=checkpoint_config,
            data_output_path=data_dir,
        )

benchmark.write_result()
