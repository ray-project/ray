from typing import Optional, Union, Tuple
import numpy as np

import ray
from ray.data.dataset import Dataset, MaterializedDataset

from benchmark import Benchmark

from typing import Literal


def map_batches(
    input_ds: Dataset,
    batch_size: int,
    batch_format: Literal["default", "pandas", "pyarrow", "numpy"],
    concurrency: Optional[Union[int, Tuple[int, int]]] = None,
    num_calls: Optional[int] = 1,
    is_eager_executed: Optional[bool] = False,
) -> Dataset:
    assert isinstance(input_ds, MaterializedDataset)
    ds = input_ds

    def udf(x):
        return x

    class UDFClass:
        def __call__(self, x):
            return x

    if concurrency is None:
        fn = udf
    else:
        fn = UDFClass

    for _ in range(num_calls):
        ds = ds.map_batches(
            fn,
            batch_format=batch_format,
            batch_size=batch_size,
            concurrency=concurrency,
        )
        if is_eager_executed:
            ds = ds.materialize()
    return ds


def run_map_batches_benchmark(benchmark: Benchmark):
    input_ds = ray.data.read_parquet(
        "s3://air-example-data/ursa-labs-taxi-data/by_year/2018/01"
    ).materialize()

    batch_formats = ["pandas", "numpy"]
    batch_sizes = [1024, 2048, 4096, None]
    num_calls_list = [1, 2, 4]

    # Test different batch_size of map_batches.
    for batch_format in batch_formats:
        for batch_size in batch_sizes:
            num_calls = 2
            test_name = f"map-batches-{batch_format}-{batch_size}-{num_calls}-eager"
            benchmark.run_materialize_ds(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                num_calls=num_calls,
                is_eager_executed=True,
            )
            test_name = f"map-batches-{batch_format}-{batch_size}-{num_calls}-lazy"
            benchmark.run_materialize_ds(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                num_calls=num_calls,
            )

    # Test multiple calls of map_batches.
    for num_calls in num_calls_list:
        for concurrency in [None, 1]:
            batch_size = 4096
            if concurrency is None:
                compute_strategy = "tasks"
            else:
                compute_strategy = "actors"

            test_name = (
                f"map-batches-{batch_format}-{batch_size}-{num_calls}-"
                f"{compute_strategy}-eager"
            )
            benchmark.run_materialize_ds(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                concurrency=concurrency,
                num_calls=num_calls,
                is_eager_executed=True,
            )
            test_name = (
                f"map-batches-{batch_format}-{batch_size}-{num_calls}-"
                f"{compute_strategy}-lazy"
            )
            benchmark.run_materialize_ds(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=batch_size,
                concurrency=concurrency,
                num_calls=num_calls,
            )

    # Test different batch formats of map_batches.
    for current_format in ["pyarrow", "pandas"]:
        new_input_ds = input_ds.map_batches(
            lambda ds: ds, batch_format=current_format, batch_size=None
        ).materialize()
        for new_format in ["pyarrow", "pandas", "numpy"]:
            for batch_size in batch_sizes:
                test_name = f"map-batches-{current_format}-to-{new_format}-{batch_size}"
                benchmark.run_materialize_ds(
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
    ).materialize()

    for batch_format in batch_formats:
        for concurrency in [None, (1, 1000)]:
            if concurrency is None:
                compute_strategy = "tasks"
            else:
                compute_strategy = "actors"
            test_name = f"map-batches-{batch_format}-{compute_strategy}-multi-files"

            benchmark.run_materialize_ds(
                test_name,
                map_batches,
                input_ds=input_ds,
                batch_format=batch_format,
                batch_size=4096,
                concurrency=concurrency,
                num_calls=1,
            )

    # Test map_batches whose output is large and will be split into multiple blocks.
    # With the following configuration, the total output data size will be
    # (num_output_blocks * 10GB).
    num_output_blocks = [10, 50, 100]
    input_size = 1024 * 1024
    batch_size = 1024
    ray.data.DataContext.get_current().target_max_block_size = 10 * 1024 * 1024
    # Disable PhysicalOperator fusion. Because we will have 2 map_batches operators.
    # And the first one will generate multiple output blocks.
    ray.data._internal.logical.optimizers.PHYSICAL_OPTIMIZER_RULES = []
    parallelism = input_size // batch_size
    input_ds = ray.data.range(input_size, parallelism=parallelism).materialize()

    def map_batches_fn(num_output_blocks, batch):
        """A map_batches function that generates num_output_blocks output blocks."""
        per_row_output_size = (
            ray.data.DataContext.get_current().target_max_block_size // len(batch["id"])
        )
        for _ in range(num_output_blocks):
            yield {
                "data": [
                    np.zeros(shape=(per_row_output_size,), dtype=np.int8)
                    for _ in range(len(batch["id"]))
                ]
            }

    for num_blocks in num_output_blocks:
        test_name = f"map-batches-multiple-output-blocks-{num_blocks}"

        return_ds = None

        def test_fn():
            nonlocal return_ds

            ds = input_ds.map_batches(
                lambda batch: map_batches_fn(num_blocks, batch),
                batch_size=batch_size,
                batch_format="numpy",
            )
            # Apply a second map_batches to caculate the output size and reduce
            # the amount of data to be materialized.
            ds = ds.map_batches(
                lambda batch: {"data_size": [sum(x.nbytes for x in batch["data"])]},
                batch_size=batch_size,
                batch_format="numpy",
            )
            ds = ds.materialize()
            return_ds = ds
            return ds

        benchmark.run_materialize_ds(
            test_name,
            test_fn,
        )

        # Check the total size of the output is correct.
        # Note, do this after the benchmark to avoid couting its time.
        total_size = sum(
            sum(batch["data_size"])
            for batch in return_ds.iter_batches(batch_size=batch_size)
        )
        expected_total_size = (
            input_size
            * num_blocks
            * ray.data.DataContext.get_current().target_max_block_size
            // batch_size
        )
        assert total_size == expected_total_size, (total_size, expected_total_size)


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("map-batches")

    run_map_batches_benchmark(benchmark)

    benchmark.write_result()
