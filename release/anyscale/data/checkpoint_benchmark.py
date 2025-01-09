import argparse
import logging
import os
import time
import numpy
from benchmark import Benchmark, BenchmarkMetric
import ray
from ray.data import DataContext
from ray.anyscale.data.checkpoint import CheckpointBackend, CheckpointConfig

logger = logging.Logger(__name__)

OUTPUT_FILE_SIZE_BYTES = 1024**3


def _parse_checkpoint_config(args: argparse.Namespace) -> CheckpointConfig:
    backend_str = args.checkpoint_backend.lower()
    if backend_str == "none":
        return CheckpointConfig(enabled=False)
    elif backend_str == "disk_batch":
        backend = CheckpointBackend.DISK_BATCH
    elif backend_str == "disk_row":
        backend = CheckpointBackend.DISK_ROW
    elif backend_str == "s3_batch":
        backend = CheckpointBackend.S3_BATCH
    elif backend_str == "s3_row":
        backend = CheckpointBackend.S3_ROW
    else:
        raise ValueError(f"Unknown checkpoint backend: {backend_str}")

    return CheckpointConfig(
        enabled=True,
        backend=backend,
        id_col="id",
        output_path=args.checkpoint_output_path,
    )


def run_checkpoints_benchmark(
    benchmark: Benchmark,
    checkpoint_config: CheckpointConfig,
    num_rows: int,
    size_bytes_per_row: int,
    transform_sleep_s: float,
    inference_sleep_s: float,
    inference_batch_size: int,
    inference_concurrency: int,
    data_output_path: str,
    benchmark_name: str = "",
):
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

    def run_ds():
        start_time = time.time()
        ds.write_parquet(
            data_output_path,
            num_rows_per_file=OUTPUT_FILE_SIZE_BYTES // size_bytes_per_row,
        )
        runtime = time.time() - start_time
        return {BenchmarkMetric.RUNTIME: runtime}

    name = "checkpoint-benchmark"
    if benchmark_name:
        name += f"-{benchmark_name}"
    name += f"-{checkpoint_config.backend}"
    benchmark.run_fn(name, run_ds)


def clean_up_output_files(
    checkpoint_config: CheckpointConfig,
    data_output_path: str,
):
    logger.info("Cleaning up output files")
    output_paths = [data_output_path]
    if checkpoint_config.enabled:
        assert checkpoint_config.output_path is not None
        output_paths.append(checkpoint_config.output_path)
    for output_path in output_paths:
        logger.info("Cleaning up %s", output_path)
        if output_path.startswith("s3://"):
            import boto3

            s3 = boto3.client("s3")
            bucket, key = output_path[len("s3://") :].split("/", 1)
            s3.delete_object(Bucket=bucket, Key=key)
        else:
            if not os.path.exists(output_path):
                continue
            import shutil

            shutil.rmtree(output_path)


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
        )
        benchmark.write_result()
    finally:
        clean_up_output_files(
            checkpoint_config,
            args.data_output_path,
        )
