import argparse
import numpy as np
from typing import Optional, Union, List

import ray
from ray.data.datastream import Dataset

from benchmark import Benchmark


def iter_torch_batches(
    ds: Dataset,
    batch_size: Optional[int] = None,
    local_shuffle_buffer_size: Optional[int] = None,
    prefetch_batches: int = 0,
    use_default_params: bool = False,
) -> Dataset:
    num_batches = 0
    if use_default_params:
        for batch in ds.iter_torch_batches():
            num_batches += 1
    else:
        for batch in ds.iter_torch_batches(
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
            prefetch_batches=prefetch_batches,
        ):
            num_batches += 1
    print(
        "iter_torch_batches done, block_format:",
        "pyarrow",
        "num_rows:",
        ds.count(),
        "num_blocks:",
        ds.num_blocks(),
        "num_batches:",
        num_batches,
    )
    return ds


def to_tf(
    ds: Dataset,
    feature_columns: Union[str, List[str]],
    label_columns: Union[str, List[str]],
    batch_size: Optional[int] = None,
    local_shuffle_buffer_size: Optional[int] = None,
    use_default_params: bool = False,
) -> Dataset:
    if use_default_params:
        ds.to_tf(feature_columns=feature_columns, label_columns=label_columns)
    else:
        ds.to_tf(
            feature_columns=feature_columns,
            label_columns=label_columns,
            batch_size=batch_size,
            local_shuffle_buffer_size=local_shuffle_buffer_size,
        )
    return ds


def run_iter_tensor_batches_benchmark(benchmark: Benchmark, data_size_gb: int):
    ds = ray.data.read_images(
        f"s3://anonymous@air-example-data-2/{data_size_gb}G-image-data-synthetic-raw"
    )

    # Add a label column.
    def add_label(batch):
        label = np.ones(shape=(len(batch), 1))
        batch["label"] = label
        return batch

    ds = ds.map_batches(add_label, batch_format="pandas").materialize()

    # Test iter_torch_batches() with default args.
    benchmark.run(
        "iter-torch-batches-default",
        iter_torch_batches,
        ds=ds,
        use_default_params=True,
    )

    # Test to_tf() with default args.
    benchmark.run(
        "to-tf-default",
        to_tf,
        ds=ds,
        feature_columns="image",
        label_columns="label",
        use_default_params=True,
    )

    batch_sizes = [16, 32]

    # Test with varying batch sizes for iter_torch_batches() and to_tf().
    for batch_size in batch_sizes:
        benchmark.run(
            f"iter-torch-batches-{batch_size}",
            iter_torch_batches,
            ds=ds,
            batch_size=batch_size,
        )
        benchmark.run(
            f"to-tf-{batch_size}",
            to_tf,
            ds=ds,
            feature_columns="image",
            label_columns="label",
            batch_size=batch_size,
        )

    prefetch_batches = [0, 1, 4]
    # Test with varying prefetching for iter_torch_batches()
    for prefetch_batch in prefetch_batches:
        for shuffle_buffer_size in [None, 64]:
            test_name = f"iter-torch-batches-bs-{32}-prefetch-{prefetch_batch}-shuffle{shuffle_buffer_size}"  # noqa: E501
            benchmark.run(
                test_name,
                iter_torch_batches,
                ds=ds,
                batch_size=32,
                prefetch_batches=prefetch_batch,
            )

    # Test with varying batch sizes and shuffle for iter_torch_batches() and to_tf().
    for batch_size in batch_sizes:
        for shuffle_buffer_size in [batch_size, 2 * batch_size]:
            test_name = f"iter-torch-batches-shuffle-{batch_size}-{shuffle_buffer_size}"
            benchmark.run(
                test_name,
                iter_torch_batches,
                ds=ds,
                batch_size=batch_size,
                local_shuffle_buffer_size=shuffle_buffer_size,
            )
            test_name = f"to-tf-shuffle-{batch_size}-{shuffle_buffer_size}"
            benchmark.run(
                test_name,
                to_tf,
                ds=ds,
                feature_columns="image",
                label_columns="label",
                batch_size=batch_size,
                local_shuffle_buffer_size=shuffle_buffer_size,
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

    args = parser.parse_args()

    benchmark = Benchmark("iter-tensor-batches")

    run_iter_tensor_batches_benchmark(benchmark, args.data_size_gb)

    benchmark.write_result()
