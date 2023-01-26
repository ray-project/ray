import pyarrow as pa

import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark
from parquet_data_generator import generate_data

import shutil
import tempfile


def read_parquet(
    root: str,
    parallelism: int = -1,
    use_threads: bool = False,
    filter=None,
    columns=None,
) -> Dataset:
    return ray.data.read_parquet(
        paths=root,
        parallelism=parallelism,
        use_threads=use_threads,
        filter=filter,
        columns=columns,
    ).fully_executed()


def run_read_parquet_benchmark(benchmark: Benchmark):
    # Test with different parallelism (multi-processing for single node) and threading.
    for parallelism in [1, 2, 4]:
        for use_threads in [True, False]:
            test_name = f"read-parquet-downsampled-nyc-taxi-2009-{parallelism}-{use_threads}"  # noqa: E501
            benchmark.run(
                test_name,
                read_parquet,
                root="s3://anonymous@air-example-data/ursa-labs-taxi-data/downsampled_2009_full_year_data.parquet",  # noqa: E501
                parallelism=parallelism,
                use_threads=use_threads,
            )

    # Test with projection and filter pushdowns.
    # Since we have projection and filter pushdown, we can run the read on the full
    # size of one year data fast enough on a single node.
    test_name = "read-parquet-nyc-taxi-2018-pushdown"
    filter_expr = (pa.dataset.field("passenger_count") <= 10) & (
        pa.dataset.field("passenger_count") > 0
    )
    benchmark.run(
        test_name,
        read_parquet,
        root="s3://anonymous@air-example-data/ursa-labs-taxi-data/by_year/2018",
        columns=["passenger_count", "trip_distance"],
        filter=filter_expr,
    )

    # Test with different number files to handle: from a few to many.
    data_dirs = []
    # Each test set has same total number of rows, which are distributed
    # to different number of files.
    total_rows = 1024 * 1024 * 8
    for num_files in [8, 128, 1024]:
        for compression in ["snappy", "gzip"]:
            data_dirs.append(tempfile.mkdtemp())
            generate_data(
                num_rows=total_rows,
                num_files=num_files,
                num_row_groups_per_file=16,
                compression=compression,
                data_dir=data_dirs[-1],
            )
            test_name = f"read-parquet-random-data-{num_files}-{compression}"
            benchmark.run(
                test_name,
                read_parquet,
                root=data_dirs[-1],
                parallelism=1,  # We are testing one task to handle N files
            )
    for dir in data_dirs:
        shutil.rmtree(dir)


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("read-parquet")

    run_read_parquet_benchmark(benchmark)

    benchmark.write_result()
