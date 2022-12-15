from typing import Literal, Optional, Union

import ray
from ray.data._internal.compute import ActorPoolStrategy, ComputeStrategy
from ray.data.dataset import Dataset

from benchmark import Benchmark


def map_batches(
    input_ds: Dataset,
    batch_size: int,
    batch_format: Literal["default", "pandas", "pyarrow", "numpy"],
    compute: Optional[Union[str, ComputeStrategy]] = None,
    num_calls: Optional[int] = 1,
) -> Dataset:

    ds = input_ds
    for _ in range(num_calls):
        ds = ds.map_batches(
            lambda x: x,
            batch_format=batch_format,
            batch_size=batch_size,
            compute=compute,
        )
    return ds


def run_map_batches_benchmark(benchmark: Benchmark):
    input_ds = ray.data.read_parquet(
        "s3://air-example-data/ursa-labs-taxi-data/by_year/2018/01"
    )
    lazy_input_ds = input_ds.lazy()

    batch_formats = ["pandas", "numpy"]
    batch_sizes = [1024, 2048, 4096, None]
    num_calls_list = [1, 2, 4]

    # Test different batch_size of map_batches.
    for batch_format in batch_formats:
        for batch_size in batch_sizes:
            # TODO(chengsu): https://github.com/ray-project/ray/issues/31108
            # Investigate why NumPy with batch_size being 1024, took much longer
            # to finish.
            if (
                batch_format == "numpy"
                and batch_size is not None
                and batch_size == 1024
            ):
                continue

            num_calls = 2
            test_name = f"map-batches-{batch_format}-{batch_size}-{num_calls}-default"
            benchmark.run(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                num_calls=num_calls,
            )
            test_name = f"map-batches-{batch_format}-{batch_size}-{num_calls}-lazy"
            benchmark.run(
                test_name,
                map_batches,
                input_ds=lazy_input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                num_calls=num_calls,
            )

    # Test multiple calls of map_batches.
    for num_calls in num_calls_list:
        for compute in ["tasks", ActorPoolStrategy(1, 1)]:
            batch_size = 4096
            if compute == "tasks":
                compute_strategy = "tasks"
            else:
                compute_strategy = "actors"

            test_name = (
                f"map-batches-{batch_format}-{batch_size}-{num_calls}-"
                f"{compute_strategy}-default"
            )
            benchmark.run(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                compute=compute,
                num_calls=num_calls,
            )
            test_name = (
                f"map-batches-{batch_format}-{batch_size}-{num_calls}-"
                f"{compute_strategy}-lazy"
            )
            benchmark.run(
                test_name,
                map_batches,
                input_ds=lazy_input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                compute=compute,
                num_calls=num_calls,
            )

    # Test different batch formats of map_batches.
    for current_format in ["pyarrow", "pandas", "numpy"]:
        new_input_ds = input_ds.map_batches(
            lambda ds: ds, batch_format=current_format, batch_size=None
        ).fully_executed()
        for new_format in ["pyarrow", "pandas", "numpy"]:
            for batch_size in batch_sizes:
                test_name = f"map-batches-{current_format}-to-{new_format}-{batch_size}"
                benchmark.run(
                    test_name,
                    map_batches,
                    input_ds=new_input_ds,
                    batch_format=new_format,
                    batch_size=batch_size,
                    num_calls=1,
                )

    # Test reading multiple files.
    input_ds = ray.data.read_parquet(
        "s3://air-example-data/ursa-labs-taxi-data/by_year/2018"
    )
    for batch_format in batch_formats:
        for compute in ["tasks", "actors"]:
            test_name = f"map-batches-{batch_format}-{compute}-multi-files"
            benchmark.run(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=4096,
                compute=compute,
                num_calls=1,
            )


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("map-batches")

    run_map_batches_benchmark(benchmark)

    benchmark.write_result()
