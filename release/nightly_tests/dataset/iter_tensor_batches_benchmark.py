from typing import Optional, Union, List

import ray
from ray.data.dataset import Dataset

from benchmark import Benchmark


def iter_torch_batches(
    ds: Dataset,
    batch_size: Optional[int] = None,
    local_shuffle_buffer_size: Optional[int] = None,
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
        ):
            num_batches += 1
    print(
        "iter_torch_batches done, block_format:",
        ds.dataset_format(),
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


def run_iter_tensor_batches_benchmark(benchmark: Benchmark):
    ds = ray.data.read_images(
        "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
    ).fully_executed()

    # Repartition both to align the block sizes so we can zip them.
    ds = ds.repartition(ds.num_blocks())
    label = ray.data.range_tensor(ds.count()).repartition(ds.num_blocks())
    ds = ds.zip(label)

    # Test iter_torch_batches() with default args.
    benchmark.run(
        "iter-torch-batches-default",
        iter_torch_batches,
        ds=ds,
        use_default_params=True,
    )

    # Test to_tf() with default params.
    benchmark.run(
        "to-tf-default",
        to_tf,
        ds=ds,
        feature_columns="image",
        label_columns="__value__",
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
            label_columns="__value__",
            batch_size=batch_size,
        )

    # Test image data with shuffle for iter_torch_batches() and to_tf().
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
                label_columns="__value__",
                batch_size=batch_size,
                local_shuffle_buffer_size=shuffle_buffer_size,
            )


if __name__ == "__main__":
    ray.init()

    benchmark = Benchmark("iter-tensor-batches")

    run_iter_tensor_batches_benchmark(benchmark)

    benchmark.write_result()
