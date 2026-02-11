import argparse
import math
import os
import random
import time
from typing import Optional

import numpy
from benchmark import Benchmark, BenchmarkMetric
from pyarrow.fs import FileSelector, FileSystem, FileType

import ray
from ray.data.checkpoint import CheckpointBackend, CheckpointConfig
from ray.data import DataContext
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data.datasource import WriteResult


def _parse_checkpoint_config(args: argparse.Namespace) -> Optional[CheckpointConfig]:
    backend_str = args.checkpoint_backend.upper()
    if backend_str == "NONE":
        return None
    elif backend_str == "FILE_STORAGE":
        backend = CheckpointBackend.FILE_STORAGE
    elif backend_str == "CLOUD_OBJECT_STORAGE":
        backend = CheckpointBackend.CLOUD_OBJECT_STORAGE
    else:
        raise ValueError(f"Unknown checkpoint backend: {backend_str}")

    if args.generated_id_column:
        id_column: Optional[str] = None
        generated_id_column: Optional[str] = args.generated_id_column
    else:
        id_column: Optional[str] = "id"
        generated_id_column: Optional[str] = None

    return CheckpointConfig(
        id_column=id_column,
        generated_id_column=generated_id_column,
        checkpoint_path=args.checkpoint_output_path,
        override_backend=backend,
    )


def run_dataset(
    checkpoint_config: Optional[CheckpointConfig],
    input_data_path: str,
    transform_sleep_s: float,
    inference_sleep_s: float,
    inference_batch_size: int,
    inference_concurrency: int,
    data_output_path: str,
    num_output_files: int,
    num_rows: Optional[int] = None,
) -> int:
    ctx = DataContext.get_current()
    ctx.checkpoint_config = checkpoint_config

    # Make read_parquet and transform fuse.
    ctx._enable_read_files_fusion_override = True

    READ_TASK_MEMORY = 8 * 1024 * 1024 * 1024  # 8GB

    ds = ray.data.read_parquet(
        input_data_path, ray_remote_args={"memory": READ_TASK_MEMORY}
    )
    if not num_rows:
        num_rows = ds.count()

    # TODO(srinathk10): Remove this once hash shuffle parallelism is system configurable.
    if num_rows == 10_000_000:
        ctx.default_hash_shuffle_parallelism = 25
    elif num_rows == 3_000_000_000:
        ctx.default_hash_shuffle_parallelism = 500

    def transform(batch):
        time.sleep(transform_sleep_s)
        return batch

    ds = ds.map_batches(transform, batch_size=None, memory=READ_TASK_MEMORY)

    class Inference:
        INFER_RESULT_DIMENSION = 16

        def __call__(self, batch):
            time.sleep(inference_sleep_s)
            batch["inference"] = numpy.random.random(
                (len(batch["data"]), self.INFER_RESULT_DIMENSION)
            )
            # Remove the data column to make the write op run faster.
            # We want the Inference op to be the main bottleneck of the pipeline.
            del batch["data"]
            return batch

    ds = ds.map_batches(
        Inference,
        batch_size=inference_batch_size,
        concurrency=inference_concurrency,
        num_gpus=1,
    )

    # Patch `on_write_complete` to get the WriteResult.
    # TODO(hchen): make `write_parquet` expose the WriteResult directly.
    num_rows_written = None
    original_on_write_complete = ParquetDatasink.on_write_complete

    def patched_on_write_complete(self, write_result: WriteResult[None]):
        nonlocal num_rows_written
        num_rows_written = write_result.num_rows
        return original_on_write_complete(self, write_result)

    ParquetDatasink.on_write_complete = patched_on_write_complete

    try:
        ds.write_parquet(
            data_output_path,
            min_rows_per_file=num_rows // num_output_files,
        )
        return int(num_rows_written)
    finally:
        ParquetDatasink.on_write_complete = original_on_write_complete


def _delete_pct_of_checkpoint_files(
    checkpoint_config: CheckpointConfig,
    percentage: float,
    *,
    recursive: bool = True,
    seed: Optional[int] = 42,
) -> int:
    """Delete a percentage of checkpoint files to test recovery scenarios."""
    if not (0.0 <= percentage <= 1.0):
        raise ValueError(f"percentage must be in [0, 1], got {percentage}")

    if seed is not None:
        random.seed(seed)

    checkpoint_path = checkpoint_config.checkpoint_path

    # Handle both local paths and S3 URIs
    if checkpoint_path.startswith(("s3://", "gs://", "file://", "http://", "https://")):
        # For URIs, get the filesystem and strip the scheme
        fs, base_path = FileSystem.from_uri(checkpoint_path)
    else:
        # For local paths, use the provided filesystem
        fs = checkpoint_config.filesystem
        base_path = checkpoint_path

    # Get list of files
    info = fs.get_file_info(base_path)
    if info.type == FileType.Directory:
        selector = FileSelector(base_path, recursive=recursive)
        file_infos = fs.get_file_info(selector)
        files = [
            checkpoint_path.rstrip("/") + "/" + fi.path[len(base_path) :].lstrip("/")
            for fi in file_infos
            if fi.type == FileType.File
        ]
    elif info.type == FileType.File:
        files = [checkpoint_path]
    else:
        files = []

    if not files:
        print(f"No checkpoint files found in {checkpoint_path}")
        return 0

    if percentage == 0.0:
        print(f"percentage=0.0, no files selected from {checkpoint_path}")
        return 0

    # Select and delete files
    num_to_delete = min(len(files), max(1, math.ceil(len(files) * percentage)))
    chosen = random.sample(files, num_to_delete)

    print(
        f"Deleting {len(chosen)} out of {len(files)} checkpoint files ({percentage*100:.0f}%)"
    )

    # Count rows before deletion
    ds = ray.data.read_parquet(chosen)
    total_rows_deleted = ds.count()
    print(f"Selected {len(chosen)} files contain {total_rows_deleted} total rows")

    # Delete files (strip s3:// prefix for filesystem operations)
    for p in chosen:
        delete_path = p[5:] if p.startswith("s3://") else p
        fs.delete_file(delete_path)

    return total_rows_deleted


def run_checkpoints_benchmark(
    benchmark: Benchmark,
    checkpoint_config: Optional[CheckpointConfig],
    input_data_path: str,
    transform_sleep_s: float,
    inference_sleep_s: float,
    inference_batch_size: int,
    inference_concurrency: int,
    data_output_path: str,
    num_output_files: int,
    benchmark_name: str = "",
    num_rows: Optional[int] = None,
):
    def run():
        start_time = time.time()
        print(f"[{benchmark_name}] Running dataset from scratch")
        if checkpoint_config is not None:
            # Keep the checkpoint files. We'll test loading them in the second run.
            checkpoint_config.delete_checkpoint_on_success = False
            print(f"checkpoint_path = {checkpoint_config.checkpoint_path}")

        num_rows_written = run_dataset(
            checkpoint_config,
            input_data_path,
            transform_sleep_s,
            inference_sleep_s,
            inference_batch_size,
            inference_concurrency,
            data_output_path,
            num_output_files,
            num_rows=num_rows,
        )
        runtime = time.time() - start_time
        print(f"[{benchmark_name}] dataset finished in {runtime:.2f} seconds")

        benchmark_results = {
            BenchmarkMetric.RUNTIME: runtime,
            BenchmarkMetric.THROUGHPUT: num_rows_written // runtime,
            "num_rows_written": num_rows_written,
        }

        if checkpoint_config is not None:
            if num_rows == 10_000_000:
                # For small scale, we test all scenarios
                test_scenarios = [
                    ("full_checkpoint", 1.0),  # Full completion
                    ("90_percent_completion", 0.90),  # 90% completion
                    ("75_percent_completion", 0.75),  # 75% completion
                    ("50_percent_completion", 0.5),  # 50% completion
                    ("25_percent_completion", 0.25),  # 25% completion
                    ("10_percent_completion", 0.1),  # 10% completion
                ]
            elif num_rows == 3_000_000_000:
                # For large scale, limit the test scenarios
                if checkpoint_config.generated_id_column is not None:
                    # For generated id column, we only test full completion and 99% completion for
                    # 3B rows scale. This is because for simulating failure, we randomly delete
                    # checkpoint files, and this may end up resulting in more partial file checkpoint
                    # completions than expected in real-world batch inference scenarios.
                    test_scenarios = [
                        ("full_checkpoint", 1.0),  # Full completion
                        ("99_percent_completion", 0.99),  # 99% completion
                    ]
                else:
                    # For existing id column, we test full completion
                    test_scenarios = [
                        ("full_checkpoint", 1.0),  # Full completion
                    ]

            for scenario_name, completion_percentage in test_scenarios:
                if completion_percentage < 1.0:
                    rows_to_recover = _delete_pct_of_checkpoint_files(
                        checkpoint_config, 1.0 - completion_percentage
                    )
                    print(
                        f"[{benchmark_name}] Testing checkpoint recovery with {completion_percentage*100:.0f}% completion, {num_rows} total rows, {rows_to_recover} rows to recover"
                    )
                else:
                    rows_to_recover = 0
                    print(
                        f"[{benchmark_name}] Rerunning dataset with full checkpoint, {num_rows} total rows"
                    )

                start_time = time.time()
                num_rows_written = run_dataset(
                    checkpoint_config,
                    input_data_path,
                    transform_sleep_s,
                    inference_sleep_s,
                    inference_batch_size,
                    inference_concurrency,
                    data_output_path,
                    num_output_files,
                    num_rows=num_rows,
                )
                assert (
                    num_rows_written == rows_to_recover
                ), f"num_rows_written: {num_rows_written}, rows_to_recover: {rows_to_recover}"
                runtime = time.time() - start_time
                benchmark_results[f"runtime_with_{scenario_name}"] = runtime
                print(
                    f"[{benchmark_name}] dataset with {scenario_name} finished in {runtime:.2f} seconds"
                )

        return benchmark_results

    benchmark.run_fn(benchmark_name, run)


def clean_up_output_files(
    checkpoint_config: Optional[CheckpointConfig],
    data_output_path: str,
):
    print("Cleaning up output files")
    output_paths = [data_output_path]
    if checkpoint_config is not None:
        assert checkpoint_config.checkpoint_path is not None
        output_paths.append(checkpoint_config.checkpoint_path)
    for checkpoint_path in output_paths:
        print(f"Cleaning up {checkpoint_path}")
        if checkpoint_path.startswith("s3://"):
            import boto3

            s3 = boto3.client("s3")
            bucket, key = checkpoint_path[len("s3://") :].split("/", 1)
            s3.delete_object(Bucket=bucket, Key=key)
        else:
            if not os.path.exists(checkpoint_path):
                continue
            import shutil

            shutil.rmtree(checkpoint_path)


# This benchmark is triggered by `run_checkpoint_benchmark.py` in CI.
# This is only used for manual run.
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--checkpoint_backend", type=str, default="None")
    _ = parser.add_argument("--input_data_path", type=str)
    _ = parser.add_argument("--data_output_path", type=str)
    _ = parser.add_argument("--checkpoint_output_path", type=str)
    _ = parser.add_argument("--inference_concurrency", type=int)
    _ = parser.add_argument("--inference_batch_size", type=int)
    _ = parser.add_argument("--inference_sleep_s", type=float)
    _ = parser.add_argument("--transform_sleep_s", type=float, default=0.001)
    _ = parser.add_argument("--num_output_files", type=int, default=50)
    _ = parser.add_argument("--generated_id_column", type=str, default=None)
    args = parser.parse_args()

    checkpoint_config = _parse_checkpoint_config(args)
    try:
        benchmark = Benchmark()
        run_checkpoints_benchmark(
            benchmark,
            checkpoint_config,
            args.input_data_path,
            args.transform_sleep_s,
            args.inference_sleep_s,
            args.inference_batch_size,
            args.inference_concurrency,
            args.data_output_path,
            args.num_output_files,
        )
        benchmark.write_result()
    finally:
        clean_up_output_files(
            checkpoint_config,
            args.data_output_path,
        )
