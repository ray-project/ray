import argparse
import numpy as np

import ray

from benchmark import Benchmark


def run_iter_tensor_batches_benchmark(
    benchmark: Benchmark, data_size_gb: int, block_size_mb: int
):
    ctx = ray.data.context.DataContext.get_current()
    ctx.target_max_block_size = block_size_mb * 1024 * 1024

    ds = ray.data.read_images(
        f"s3://anonymous@air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    )

    # Add a label column.
    def add_label(batch):
        label = np.ones(shape=(len(batch), 1))
        batch["label"] = label
        return batch

    ds = ds.map_batches(add_label, batch_format="pandas")

    # Test iter_torch_batches() with default args.
    benchmark.run_iterate_ds(
        "iter-torch-batches-default",
        ds.iter_torch_batches(),
    )

    # Test to_tf() with default args.
    benchmark.run_iterate_ds(
        "to-tf-default", ds.to_tf(feature_columns="image", label_columns="label")
    )

    batch_sizes = [16, 32]

    # Test with varying batch sizes for iter_torch_batches() and to_tf().
    for batch_size in batch_sizes:
        benchmark.run_iterate_ds(
            f"iter-torch-batches-{batch_size}-block-size-{block_size_mb}",
            ds.iter_torch_batches(batch_size=batch_size),
        )

    prefetch_batches = [0, 1, 4]
    # Test with varying prefetching for iter_torch_batches()
    for prefetch_batch in prefetch_batches:
        for shuffle_buffer_size in [None, 64]:
            test_name = f"iter-torch-batches-bs-{32}-prefetch-{prefetch_batch}-shuffle{shuffle_buffer_size}"  # noqa: E501
            benchmark.run_iterate_ds(
                test_name,
                ds.iter_torch_batches(batch_size=32, prefetch_batches=prefetch_batch),
            )

    # Test with varying batch sizes and shuffle for iter_torch_batches() and to_tf().
    for batch_size in batch_sizes:
        for shuffle_buffer_size in [batch_size, 2 * batch_size]:
            test_name = f"iter-torch-batches-shuffle-{batch_size}-{shuffle_buffer_size}"
            benchmark.run_iterate_ds(
                test_name,
                ds.iter_torch_batches(
                    batch_size=batch_size, local_shuffle_buffer_size=shuffle_buffer_size
                ),
            )
            test_name = f"to-tf-shuffle-{batch_size}-{shuffle_buffer_size}"
            benchmark.run_iterate_ds(
                test_name,
                ds.to_tf(
                    feature_columns="image",
                    label_columns="label",
                    batch_size=batch_size,
                    local_shuffle_buffer_size=shuffle_buffer_size,
                ),
            )


if __name__ == "__main__":
    ray.init()

    parser = argparse.ArgumentParser(
        description="Helper script to upload files to S3 bucket"
    )
    parser.add_argument(
        "--data-size-gb",
        choices=[1, 10],
        type=int,
        default=1,
        help="The data size to use for the dataset.",
    )
    parser.add_argument(
        "--block-size-mb",
        type=int,
        default=128,
        help="The data size to use for the dataset.",
    )

    args = parser.parse_args()

    benchmark = Benchmark("iter-tensor-batches")

    run_iter_tensor_batches_benchmark(benchmark, args.data_size_gb, args.block_size_mb)

    benchmark.write_result()
