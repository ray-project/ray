import argparse
import tempfile
import time

import numpy as np
import pyarrow as pa
import torch
import torch.distributed as dist

from benchmark import Benchmark, BenchmarkMetric

import ray
import ray.data
import ray.train
from ray.data import MixStoppingCondition
from ray.train import Checkpoint, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

IMAGENET_TRAIN_PATH = (
    "s3://ray-benchmark-data-internal-us-west-2/imagenet/parquet_split/train"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dataset.mix() benchmark")
    parser.add_argument("--num-datasets", type=int, default=2)
    parser.add_argument("--weights", nargs="+", type=float, default=None)
    parser.add_argument("--num-workers", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--max-rows-per-worker", type=int, default=None)
    parser.add_argument(
        "--stopping-condition",
        default="stop_on_longest_drop",
        choices=["stop_on_shortest", "stop_on_longest_drop"],
    )
    parser.add_argument("--random-mix", action="store_true")
    parser.add_argument("--print-every", type=int, default=50)
    return parser.parse_args()


def _create_dataset(ds_index: int) -> ray.data.Dataset:
    ds = ray.data.read_parquet(IMAGENET_TRAIN_PATH, columns=["image", "label"])

    def preprocess(row):
        row["ds_index"] = np.int64(ds_index)
        return row

    ds = ds.map(preprocess)
    return ds


def _random_shuffle_fn(batch: pa.Table) -> pa.Table:
    indices = np.random.permutation(len(batch))
    return batch.take(indices)


def main(args):
    benchmark = Benchmark()

    stopping = MixStoppingCondition(args.stopping_condition)
    weights = args.weights or [1.0] * args.num_datasets

    if len(weights) != args.num_datasets:
        raise ValueError(
            f"Number of weights ({len(weights)}) must match "
            f"--num-datasets ({args.num_datasets})"
        )

    total_weight = sum(weights)
    normalized_weights = [w / total_weight for w in weights]

    local_batch_size = args.batch_size
    # Hard code some sensible values for the target block size and shuffle buffer size.
    target_block_size = 4 * local_batch_size
    shuffle_buffer_size = 64 * local_batch_size

    datasets = [_create_dataset(i) for i in range(args.num_datasets)]
    datasets = [
        ds.repartition(target_num_rows_per_block=target_block_size) for ds in datasets
    ]
    first, *rest = datasets
    mixed = first.mix(*rest, weights=weights, stopping_condition=stopping)

    if args.random_mix:
        mixed = mixed.map_batches(
            _random_shuffle_fn,
            batch_size=shuffle_buffer_size,
            batch_format="pyarrow",
        )

    def benchmark_fn():
        def train_fn(config):
            num_ds = config["num_datasets"]
            batch_size = config["batch_size"]
            max_rows = config.get("max_rows_per_worker")
            print_every = config.get("print_every", 50)
            is_rank_0 = ray.train.get_context().get_world_rank() == 0

            shard = ray.train.get_dataset_shard("train")

            local_rows = 0
            num_batches = 0
            count_history = [[] for _ in range(num_ds)]
            batch_size_history = []

            def compute_global_ratio_mean_stdev():
                # All-reduce batch sizes once (same across all datasets).
                batch_size_sums = torch.tensor(batch_size_history, dtype=torch.double)
                dist.all_reduce(batch_size_sums, op=dist.ReduceOp.SUM)

                ratio_means = []
                ratio_stdevs = []
                for i in range(num_ds):
                    count_sums = torch.tensor(count_history[i], dtype=torch.double)
                    dist.all_reduce(count_sums, op=dist.ReduceOp.SUM)

                    ratios = count_sums.numpy() / batch_size_sums.numpy()
                    ratio_means.append(np.mean(ratios))
                    ratio_stdevs.append(np.std(ratios))

                return ratio_means, ratio_stdevs

            start = time.perf_counter()

            for batch in shard.iter_batches(batch_size=batch_size):
                num_batches += 1
                batch_size_actual = len(batch["ds_index"])
                local_rows += batch_size_actual

                indices, counts = np.unique(batch["ds_index"], return_counts=True)
                batch_size_history.append(batch_size_actual)
                for i in range(num_ds):
                    mask = indices == i
                    count_history[i].append(counts[mask][0] if mask.any() else 0)

                if num_batches % print_every == 0:
                    ratio_means, ratio_stdevs = compute_global_ratio_mean_stdev()

                    if is_rank_0:
                        avg_str = ", ".join(
                            f"ds{i}: {ratio_means[i]:.3f}±{ratio_stdevs[i]:.3f}"
                            for i in range(num_ds)
                        )
                        print(f"[Global] Batch {num_batches}: avg={avg_str}")

                if max_rows is not None and local_rows >= max_rows:
                    break

            elapsed = time.perf_counter() - start

            ratio_means, ratio_stdevs = compute_global_ratio_mean_stdev()

            # Throughput: total rows / max elapsed across workers.
            local_rows_tensor = torch.tensor([local_rows], dtype=torch.long)
            max_elapsed = torch.tensor([elapsed], dtype=torch.double)
            dist.all_reduce(local_rows_tensor, op=dist.ReduceOp.SUM)
            dist.all_reduce(max_elapsed, op=dist.ReduceOp.MAX)

            total_rows = local_rows_tensor.item()
            global_tput = (
                total_rows / max_elapsed.item() if max_elapsed.item() > 0 else 0
            )

            metrics = {
                "global_rows": total_rows,
                "global_tput": global_tput,
                "num_batches": num_batches,
            }
            for i in range(num_ds):
                metrics[f"ratio_mean_ds{i}"] = ratio_means[i]
                metrics[f"ratio_std_ds{i}"] = ratio_stdevs[i]

            with tempfile.TemporaryDirectory() as temp_dir:
                ray.train.report(
                    metrics, checkpoint=Checkpoint.from_directory(temp_dir)
                )

        trainer = TorchTrainer(
            train_fn,
            train_loop_config={
                "batch_size": args.batch_size,
                "num_datasets": args.num_datasets,
                "max_rows_per_worker": args.max_rows_per_worker,
                "print_every": args.print_every,
            },
            scaling_config=ScalingConfig(
                num_workers=args.num_workers,
                use_gpu=False,
                placement_strategy="SPREAD",
            ),
            datasets={"train": mixed},
            run_config=RunConfig(storage_path="/mnt/cluster_storage"),
        )

        result = trainer.fit()

        output = vars(args)
        output[BenchmarkMetric.THROUGHPUT] = result.metrics["global_tput"]
        output[BenchmarkMetric.NUM_ROWS] = result.metrics["global_rows"]

        for i in range(args.num_datasets):
            for prefix in ("ratio_mean_ds", "ratio_std_ds"):
                key = f"{prefix}{i}"
                if key in result.metrics:
                    output[key] = result.metrics[key]

        return output

    benchmark.run_fn("main", benchmark_fn)
    benchmark.write_result()

    # Assert ratio correctness after writing results.
    MEAN_THRESHOLD = 0.05
    STDEV_THRESHOLD = 0.15

    result_metrics = benchmark.result["main"]
    for i in range(args.num_datasets):
        mean_key = f"ratio_mean_ds{i}"
        std_key = f"ratio_std_ds{i}"
        if mean_key in result_metrics:
            expected = normalized_weights[i]
            actual = result_metrics[mean_key]
            std = result_metrics.get(std_key, 0)
            diff = abs(actual - expected)

            assert diff < MEAN_THRESHOLD, (
                f"Ratio for dataset {i}: expected {expected:.4f}, "
                f"got {actual:.4f} (diff={diff:.4f} exceeds threshold {MEAN_THRESHOLD})"
            )
            assert (
                std < STDEV_THRESHOLD
            ), f"Ratio std for dataset {i}: {std:.4f} exceeds threshold {STDEV_THRESHOLD}"
            print(
                f"Dataset {i}: mean={actual:.4f}±{std:.4f}, "
                f"target={expected:.4f}, diff={diff:.4f} OK"
            )


if __name__ == "__main__":
    ray.init()
    args = parse_args()
    main(args)
