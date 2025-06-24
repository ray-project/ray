import gc

import pytest

import ray
from ray.data import DataContext
from ray.data._internal.util import GiB
from ray.data.tests.test_arrow_block import (
    parquet_dataset_single_column_gt_2gb,  # noqa: F401
)


@pytest.mark.parametrize(
    "op",
    [
        "map",
        "map_batches",
    ],
)
def test_arrow_batch_gt_2gb(
    ray_start_regular,
    parquet_dataset_single_column_gt_2gb,  # noqa: F811
    restore_data_context,
    op,
):
    # Disable (automatic) fallback to `ArrowPythonObjectType` extension type
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    dataset_path, num_rows, total_column_size = parquet_dataset_single_column_gt_2gb

    def _id(x):
        return x

    ds = ray.data.read_parquet(dataset_path)

    if op == "map":
        ds = ds.map(_id)
    elif op == "map_batches":
        # Combine all rows into a single batch using `map_batches` coercing to
        # numpy format
        ds = ds.map_batches(
            _id,
            batch_format="numpy",
            batch_size=num_rows,
            zero_copy_batch=False,
        )

    batch = ds.take_batch()

    total_binary_column_size = sum([len(b) for b in batch["bin"]])

    print(
        f">>> Batch:\n"
        f"------\n"
        "Column: 'id'\n"
        f"Values: {batch['id']}\n"
        f"------\n"
        "Column: 'bin'\n"
        f"Total: {total_binary_column_size / GiB} GiB\n"
        f"Values: {[str(v)[:3] + ' x ' + str(len(v)) for v in batch['bin']]}\n"
    )

    assert total_binary_column_size == total_column_size

    # Clean up refs
    del batch
    del ds
    # Force GC to free up object store memory
    gc.collect()
