import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark
from parquet_data_generator import generate_data

import shutil
import tempfile
from typing import Optional


def read_parquet(
    root: str,
    override_num_blocks: Optional[int] = None,
    use_threads: bool = False,
    filter=None,
    columns=None,
) -> Dataset:
    return ray.data.read_parquet(
        paths=root,
        override_num_blocks=override_num_blocks,
        use_threads=use_threads,
        filter=filter,
        columns=columns,
    )


def run_read_parquet_benchmark(benchmark: Benchmark):
    # Test with different override_num_blocks (multi-processing for single node)
    # and threading.
    for override_num_blocks in [1, 2, 4]:
        for use_threads in [True, False]:
            test_name = f"read-parquet-downsampled-nyc-taxi-2009-{override_num_blocks}-{use_threads}"  # noqa: E501
            benchmark.run_materialize_ds(
                test_name,
                read_parquet,
                root="s3://anonymous@air-example-data/ursa-labs-taxi-data/downsampled_2009_full_year_data.parquet",  # noqa: E501
                override_num_blocks=override_num_blocks,
                use_threads=use_threads,
            )

    # TODO: Test below is currently excluded, due to failure around
    # pickling the Dataset involving the filter expression.
    # The error is present on Python < 3.8, and involves the pickle/pickle5
    # libraries. `pickle` is included as a default library from Python 3.8+,
    # whereas Python versions before this must import the backported `pickle5` library
    # to maintain the same functionality.

    # Test with projection and filter pushdowns.
    # Since we have projection and filter pushdown, we can run the read on the full
    # size of one year data fast enough on a single node.
    # test_name = "read-parquet-nyc-taxi-2018-pushdown"
    # filter_expr = (pa.dataset.field("passenger_count") <= 10) & (
    #     pa.dataset.field("passenger_count") > 0
    # )
    # benchmark.run(
    #     test_name,
    #     read_parquet,
    #     root="s3://anonymous@air-example-data/ursa-labs-taxi-data/by_year/2018",
    #     columns=["passenger_count", "trip_distance"],
    #     filter=filter_expr,
    # )

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
            benchmark.run_materialize_ds(
                test_name,
                read_parquet,
                root=data_dirs[-1],
                override_num_blocks=1,  # We are testing one task to handle N files
            )
    for dir in data_dirs:
        shutil.rmtree(dir)

    # Test reading many small files.
    num_files = 1000
    num_row_groups_per_file = 2
    total_rows = num_files * num_row_groups_per_file
    compression = "gzip"

    many_files_dir = "s3://air-example-data-2/read-many-parquet-files/"
    # If needed, use the following utility to generate files on S3.
    # Otherwise, the benchmark will read pre-generated files in the above bucket.
    # generate_data(
    #     num_rows=total_rows,
    #     num_files=num_files,
    #     num_row_groups_per_file=num_row_groups_per_file,
    #     compression=compression,
    #     data_dir=many_files_dir,
    # )
    test_name = f"read-many-parquet-files-s3-{num_files}-{compression}"
    benchmark.run_materialize_ds(
        test_name,
        read_parquet,
        root=many_files_dir,
    )


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("read-parquet")

    run_read_parquet_benchmark(benchmark)

    benchmark.write_result()
