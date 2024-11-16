import os
from tempfile import TemporaryDirectory

import ray
import sys
import pytest
from pyarrow import parquet as pq
import pyarrow as pa

from ray.data import DataContext
from ray.data._internal.util import GiB, MiB


@pytest.fixture(scope="module")
def parquet_dataset_single_column_gt_2gb():
    chunk_size = 256 * MiB
    num_chunks = 10

    total_column_size = chunk_size * 10 # ~2.5 GiB

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


@pytest.mark.parametrize(
    "op", ["map", "map_batches"]
)
def test_arrow_batch_gt_2gb(binary_dataset_gt_2gb_single_file, restore_data_context, op):
    # Disable (automatic) fallback to `ArrowPythonObjectType` extension type
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    dataset_path, num_rows, total_column_size = binary_dataset_gt_2gb_single_file

    def _id(x):
        print(f">>> [DBG] _id: {len(str(x)) / GiB:.2f} GiB, {str(x)[:128]}")
        return x

    ds = ray.data.read_parquet(dataset_path).materialize()

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

    total_binary_column_size = sum([len(b) for b in batch['bin']])

    print(
        f">>> Batch:\n"
        f"------\n"
        "Column: 'id'"
        f"Values: {batch['id']}\n"
        f"------\n"
        "Column: 'bin'"
        f"Total: {total_binary_column_size / GiB} GiB\n"
        f"Values: {[str(v)[:3] + ' x ' + str(len(v)) for v in batch['bin']]}\n"
    )

    assert total_binary_column_size == total_column_size


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
