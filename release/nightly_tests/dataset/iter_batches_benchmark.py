import sys
from typing import Optional

import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


def iter_batches(
    ds: Dataset,
    batch_size: Optional[int] = None,
    batch_format: Literal["default", "pandas", "pyarrow", "numpy"] = "default",
    local_shuffle_buffer_size: Optional[int] = None,
    use_default_params: bool = False,
) -> Dataset:
    num_batches = 0
    if use_default_params:
        for batch in ds.iter_batches():
            num_batches += 1
    else:
        for batch in ds.iter_batches(
            batch_format=batch_format,
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
        ):
            num_batches += 1
    print(
        "iter_batches done, block_format:",
        ds.dataset_format(),
        "batch_format:",
        batch_format,
        "num_rows:",
        ds.count(),
        "num_blocks:",
        ds.num_blocks(),
        "num_batches:",
        num_batches,
    )
    return ds


def run_iter_batches_benchmark(benchmark: Benchmark):
    # Total number of rows: 8,759,874
    # Avg rows per block: 729,989
    ds = (
        ray.data.read_parquet(
            "s3://anonymous@air-example-data/ursa-labs-taxi-data/by_year/2018/01"
        )
        .repartition(12)
        .fully_executed()
    )

    batch_formats = ["pandas", "numpy"]
    batch_sizes = [4 * 1024, 16 * 1024, 64 * 1024]

    # Test default args.
    test_name = "iter-batches-default"
    benchmark.run(
        test_name,
        iter_batches,
        ds=ds,
        use_default_params=True,
    )

    # Test different batch format conversions.
    for current_format in ["pyarrow", "pandas"]:
        new_ds = ds.map_batches(
            lambda ds: ds, batch_format=current_format, batch_size=None
        ).fully_executed()
        for new_format in ["pyarrow", "pandas", "numpy"]:
            for batch_size in batch_sizes:
                test_name = f"iter-batches-conversion-{current_format}-to-{new_format}-{batch_size}"  # noqa: E501
                benchmark.run(
                    test_name,
                    iter_batches,
                    ds=new_ds,
                    batch_format=new_format,
                    batch_size=batch_size,
                )

    # Test local shuffle with different buffer sizes.
    for batch_format in batch_formats:
        for batch_size in batch_sizes:
            for shuffle_buffer_size in [batch_size, 2 * batch_size, 4 * batch_size]:
                test_name = f"iter-batches-shuffle-{batch_format}-{batch_size}-{shuffle_buffer_size}"  # noqa: E501
                benchmark.run(
                    test_name,
                    iter_batches,
                    ds=ds,
                    batch_size=batch_size,
                    batch_format=batch_format,
                    local_shuffle_buffer_size=shuffle_buffer_size,
                )

    # Test block concatnation to create batches.
    # Total number of rows: 8,759,874
    # Avg rows per block: 17,108
    new_ds = ds.repartition(512)
    new_ds = new_ds.map_batches(
        lambda ds: ds, batch_format="pandas", batch_size=None
    ).fully_executed()
    print("ds:", new_ds)
    for batch_size in [32 * 1024, 64 * 1024, 256 * 1024]:
        test_name = f"iter-batches-block-concat-to-batch-{batch_size}"
        benchmark.run(
            test_name,
            iter_batches,
            ds=new_ds,
            batch_format="pandas",
            batch_size=batch_size,
        )

    # TODO(jian): test the prefetch as we set up multi-node cluster test.


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("iter-batches")

    run_iter_batches_benchmark(benchmark)

    benchmark.write_result()
