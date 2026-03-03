import gc
import os
import sys
from tempfile import TemporaryDirectory

import pyarrow as pa
import pytest
from pyarrow import parquet as pq

import ray
from ray.data._internal.util import GiB, MiB
from ray.data.context import DataContext
from ray.tests.conftest import _ray_start


@pytest.fixture(scope="module")
def parquet_dataset_single_column_gt_2gb():
    chunk_size = 256 * MiB
    num_chunks = 10

    total_column_size = chunk_size * 10  # ~2.5 GiB

    with TemporaryDirectory() as tmp_dir:
        dataset_path = f"{tmp_dir}/large_parquet_chunk_{chunk_size}"

        # Create directory
        os.mkdir(dataset_path)

        for i in range(num_chunks):
            chunk = b"a" * chunk_size

            d = {"id": [i], "bin": [chunk]}
            t = pa.Table.from_pydict(d)

            print(f">>> Table schema: {t.schema} (size={sys.getsizeof(t)})")

            filepath = f"{dataset_path}/chunk_{i}.parquet"
            pq.write_table(t, filepath)

            print(f">>> Created a chunk #{i}")

        print(f">>> Created dataset at {dataset_path}")

        yield dataset_path, num_chunks, total_column_size

        print(f">>> Cleaning up dataset at {dataset_path}")


@pytest.fixture(scope="module")
def ray_cluster_3gb_object_store():
    original_limit = ray._private.ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT

    ray._private.ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT = 3 * GiB

    with _ray_start(object_store_memory=3 * GiB) as res:
        yield res

    ray._private.ray_constants.MAC_DEGRADED_PERF_MMAP_SIZE_LIMIT = original_limit


@pytest.mark.parametrize(
    "op",
    [
        "map",
        "map_batches",
    ],
)
@pytest.mark.timeout(300)
def test_arrow_batch_gt_2gb(
    ray_cluster_3gb_object_store,
    parquet_dataset_single_column_gt_2gb,
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
            batch_format="pyarrow",
            batch_size=num_rows,
            zero_copy_batch=True,
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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
