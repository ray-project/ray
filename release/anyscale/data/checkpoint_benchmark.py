import argparse
import logging
import os
import time
from typing import Optional
import numpy
from ray.data.datasource import WriteResult
from benchmark import Benchmark, BenchmarkMetric
import ray
from ray.exceptions import UserCodeException
from ray.data import DataContext
from ray.anyscale.data.checkpoint import CheckpointBackend, CheckpointConfig

from ray.data._internal.datasource.parquet_datasink import ParquetDatasink

logger = logging.Logger(__name__)


def _parse_checkpoint_config(args: argparse.Namespace) -> Optional[CheckpointConfig]:
    backend_str = args.checkpoint_backend.upper()
    if backend_str == "NONE":
        return None
    elif backend_str == "FILE_STORAGE":
        backend = CheckpointBackend.FILE_STORAGE
    elif backend_str == "FILE_STORAGE_ROW":
        backend = CheckpointBackend.FILE_STORAGE_ROW
    elif backend_str == "CLOUD_OBJECT_STORAGE":
        backend = CheckpointBackend.CLOUD_OBJECT_STORAGE
    elif backend_str == "CLOUD_OBJECT_STORAGE_ROW":
        backend = CheckpointBackend.CLOUD_OBJECT_STORAGE_ROW
    else:
        raise ValueError(f"Unknown checkpoint backend: {backend_str}")

    return CheckpointConfig(
        id_column="id",
        checkpoint_path=args.checkpoint_output_path,
        override_backend=backend,
    )


def run_dataset(
    checkpoint_config: Optional[CheckpointConfig],
    num_rows: int,
    size_bytes_per_row: int,
    transform_sleep_s: float,
    inference_sleep_s: float,
    inference_batch_size: int,
    inference_concurrency: int,
    data_output_path: str,
    fraction_checkpointed: Optional[float],
    num_output_files: int,
) -> int:
    ctx = DataContext.get_current()
    ctx.checkpoint_config = checkpoint_config

    ds = ray.data.range(num_rows)

    def gen_data(row):
        row["data"] = numpy.zeros(size_bytes_per_row, dtype=numpy.int8)
        return row

    ds = ds.map(gen_data)

    def transform(row):
        time.sleep(transform_sleep_s)
        return row

    ds = ds.map(transform)

    class Inference:
        INFER_RESULT_DIMENSION = 128

        def __call__(self, batch):
            if (
                fraction_checkpointed
                and batch["id"][0] > num_rows * fraction_checkpointed
            ):
                raise RuntimeError("Inference failed")
            time.sleep(inference_sleep_s)
            batch["inference"] = numpy.random.random(
                (len(batch["data"]), self.INFER_RESULT_DIMENSION)
            )
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
        return num_rows_written
    finally:
        ParquetDatasink.on_write_complete = original_on_write_complete


def run_checkpoints_benchmark(
    benchmark: Benchmark,
    checkpoint_config: Optional[CheckpointConfig],
    num_rows: int,
    size_bytes_per_row: int,
    transform_sleep_s: float,
    inference_sleep_s: float,
    inference_batch_size: int,
    inference_concurrency: int,
    data_output_path: str,
    fraction_checkpointed: Optional[float],
    num_output_files: int,
    benchmark_name: str = "",
):
    def run():
        if fraction_checkpointed is not None:
            try:
                run_dataset(
                    checkpoint_config,
                    num_rows,
                    size_bytes_per_row,
                    transform_sleep_s,
                    inference_sleep_s,
                    inference_batch_size,
                    inference_concurrency,
                    data_output_path,
                    fraction_checkpointed,
                    num_output_files,
                )
            except UserCodeException:
                pass

        start_time = time.time()
        num_rows_written = run_dataset(
            checkpoint_config,
            num_rows,
            size_bytes_per_row,
            transform_sleep_s,
            inference_sleep_s,
            inference_batch_size,
            inference_concurrency,
            data_output_path,
            None,
            num_output_files,
        )
        runtime = time.time() - start_time
        return {
            BenchmarkMetric.RUNTIME: runtime,
            BenchmarkMetric.THROUGHPUT: num_rows_written // runtime,
        }

    benchmark.run_fn(benchmark_name, run)


def clean_up_output_files(
    checkpoint_config: Optional[CheckpointConfig],
    data_output_path: str,
):
    logger.info("Cleaning up output files")
    output_paths = [data_output_path]
    if checkpoint_config is not None:
        assert checkpoint_config.checkpoint_path is not None
        output_paths.append(checkpoint_config.checkpoint_path)
    for checkpoint_path in output_paths:
        logger.info("Cleaning up %s", checkpoint_path)
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
    parser.add_argument("--checkpoint_backend", type=str, default="None")
    parser.add_argument("--data_output_path", type=str)
    parser.add_argument("--checkpoint_output_path", type=str)
    parser.add_argument("--inference_concurrency", type=int)
    parser.add_argument("--num_rows", type=int)
    parser.add_argument("--size_bytes_per_row", type=int)
    parser.add_argument("--inference_batch_size", type=int)
    parser.add_argument("--inference_sleep_s", type=float)
    parser.add_argument("--transform_sleep_s", type=float, default=0.001)
    parser.add_argument(
        "--fraction_checkpointed",
        type=float,
        default=None,
        help="Fraction of data that has already been checkpointed.",
    )
    parser.add_argument("--num_output_files", type=int, default=50)
    args = parser.parse_args()

    checkpoint_config = _parse_checkpoint_config(args)
    try:
        benchmark = Benchmark()
        run_checkpoints_benchmark(
            benchmark,
            checkpoint_config,
            args.num_rows,
            args.size_bytes_per_row,
            args.transform_sleep_s,
            args.inference_sleep_s,
            args.inference_batch_size,
            args.inference_concurrency,
            args.data_output_path,
            args.fraction_checkpointed,
            args.num_output_files,
        )
        benchmark.write_result()
    finally:
        clean_up_output_files(
            checkpoint_config,
            args.data_output_path,
        )
