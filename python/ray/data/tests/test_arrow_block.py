import gc
import os
import sys
import types
from tempfile import TemporaryDirectory
from typing import Union

import numpy as np
import pyarrow as pa
import pytest
from pyarrow import parquet as pq

import ray
from ray._private.test_utils import run_string_as_driver
from ray.air.util.tensor_extensions.arrow import ArrowTensorArray
from ray.data import DataContext
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops.transform_pyarrow import combine_chunked_array
from ray.data._internal.util import GiB, MiB
from ray.data.extensions.object_extension import _object_extension_type_allowed


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
def binary_dataset_single_file_gt_2gb():
    total_size = int(2.1 * GiB)
    chunk_size = 256 * MiB
    num_chunks = total_size // chunk_size
    remainder = total_size % chunk_size

    with TemporaryDirectory() as tmp_dir:
        dataset_path = f"{tmp_dir}/binary_dataset_gt_2gb_single_file"

        # Create directory
        os.mkdir(dataset_path)

        with open(f"{dataset_path}/chunk.bin", "wb") as f:
            for i in range(num_chunks):
                f.write(b"a" * chunk_size)

                print(f">>> Written chunk #{i}")

            if remainder:
                f.write(b"a" * remainder)

        print(f">>> Wrote chunked dataset at: {dataset_path}")

        yield dataset_path, total_size

        print(f">>> Cleaning up dataset: {dataset_path}")


@pytest.mark.parametrize(
    "col_name",
    [
        "bytes",
        # TODO fix numpy conversion
        # "text",
    ],
)
def test_single_row_gt_2gb(
    ray_start_regular,
    restore_data_context,
    binary_dataset_single_file_gt_2gb,
    col_name,
):
    # Disable (automatic) fallback to `ArrowPythonObjectType` extension type
    DataContext.get_current().enable_fallback_to_arrow_object_ext_type = False

    dataset_path, target_binary_size = binary_dataset_single_file_gt_2gb

    def _id(row):
        bs = row[col_name]
        assert round(len(bs) / GiB, 1) == round(target_binary_size / GiB, 1)
        return row

    if col_name == "text":
        ds = ray.data.read_text(dataset_path)
    elif col_name == "bytes":
        ds = ray.data.read_binary_files(dataset_path)

    total = ds.map(_id).count()

    assert total == 1


@pytest.mark.parametrize(
    "op",
    [
        "map",
        "map_batches",
    ],
)
def test_arrow_batch_gt_2gb(
    ray_start_regular,
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


@pytest.mark.parametrize(
    "input_,expected_output",
    [
        # Empty chunked array
        (pa.chunked_array([], type=pa.int8()), pa.array([], type=pa.int8())),
        # Fixed-shape tensors
        (
            pa.chunked_array(
                [
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                ]
            ),
            ArrowTensorArray.from_numpy(
                np.concatenate(
                    [
                        np.arange(3).reshape(3, 1),
                        np.arange(3).reshape(3, 1),
                    ]
                )
            ),
        ),
        # Ragged (variable-shaped) tensors
        (
            pa.chunked_array(
                [
                    ArrowTensorArray.from_numpy(np.arange(3).reshape(3, 1)),
                    ArrowTensorArray.from_numpy(np.arange(5).reshape(5, 1)),
                ]
            ),
            ArrowTensorArray.from_numpy(
                np.concatenate(
                    [
                        np.arange(3).reshape(3, 1),
                        np.arange(5).reshape(5, 1),
                    ]
                )
            ),
        ),
        # Small (< 2 GiB) arrays
        (
            pa.chunked_array(
                [
                    pa.array([1, 2, 3], type=pa.int16()),
                    pa.array([4, 5, 6], type=pa.int16()),
                ]
            ),
            pa.array([1, 2, 3, 4, 5, 6], type=pa.int16()),
        ),
    ],
)
def test_combine_chunked_array_small(
    input_, expected_output: Union[pa.Array, pa.ChunkedArray]
):
    result = combine_chunked_array(input_)

    expected_output.equals(result)


def test_combine_chunked_array_large():
    """Verifies `combine_chunked_array` on arrays > 2 GiB"""

    # 144 MiB
    ones_1gb = np.ones(shape=(550, 128, 128, 4), dtype=np.int32()).ravel()

    # Total ~2.15 GiB
    input_ = pa.chunked_array(
        [
            pa.array(ones_1gb),
        ]
        * 16
    )

    assert round(input_.nbytes / GiB, 2) == 2.15

    result = combine_chunked_array(input_)

    assert isinstance(result, pa.ChunkedArray)
    assert len(result.chunks) == 2

    # Should re-combine first provided 14 chunks into 1
    assert result.chunks[0].nbytes == sum([c.nbytes for c in input_.chunks[:14]])
    # Remaining 2 go into the second one
    assert result.chunks[1].nbytes == sum([c.nbytes for c in input_.chunks[14:]])


def test_append_column(ray_start_regular_shared):
    animals = ["Flamingo", "Centipede"]
    num_legs = [2, 100]
    block = pa.Table.from_pydict({"animals": animals})

    block_accessor = ArrowBlockAccessor.for_block(block)
    actual_block = block_accessor.append_column("num_legs", num_legs)

    expected_block = pa.Table.from_pydict({"animals": animals, "num_legs": num_legs})
    assert actual_block.equals(expected_block)


def test_register_arrow_types(tmp_path):
    # Test that our custom arrow extension types are registered on initialization.
    ds = ray.data.from_items(np.zeros((8, 8, 8), dtype=np.int64))
    tmp_file = f"{tmp_path}/test.parquet"
    ds.write_parquet(tmp_file)

    ds = ray.data.read_parquet(tmp_file)
    schema = (
        "Column  Type\n------  ----\nitem    numpy.ndarray(shape=(8, 8), dtype=int64)"
    )
    assert str(ds.schema()) == schema

    # Also run in driver script to eliminate existing imports.
    driver_script = """import ray
ds = ray.data.read_parquet("{0}")
schema = ds.schema()
assert str(schema) == \"\"\"{1}\"\"\"
""".format(
        tmp_file, schema
    )
    run_string_as_driver(driver_script)


@pytest.mark.skipif(
    not _object_extension_type_allowed(), reason="Object extension type not supported."
)
def test_dict_doesnt_fallback_to_pandas_block(ray_start_regular_shared):
    # If the UDF returns a column with dict, previously, we would
    # fall back to pandas, because we couldn't convert it to
    # an Arrow block. This test checks that the block
    # construction now correctly goes to Arrow.
    def fn(batch):
        batch["data_dict"] = [{"data": 0} for _ in range(len(batch["id"]))]
        batch["data_objects"] = [
            types.SimpleNamespace(a=1, b="test") for _ in range(len(batch["id"]))
        ]
        return batch

    ds = ray.data.range(10).map_batches(fn)
    ds = ds.materialize()
    block = ray.get(ds.get_internal_block_refs()[0])
    assert isinstance(block, pa.Table), type(block)
    df_from_block = block.to_pandas()
    assert df_from_block["data_dict"].iloc[0] == {"data": 0}
    assert df_from_block["data_objects"].iloc[0] == types.SimpleNamespace(a=1, b="test")

    def fn2(batch):
        batch["data_none"] = [None for _ in range(len(batch["id"]))]
        return batch

    ds2 = ray.data.range(10).map_batches(fn2)
    ds2 = ds2.materialize()
    block = ray.get(ds2.get_internal_block_refs()[0])
    assert isinstance(block, pa.Table), type(block)
    df_from_block = block.to_pandas()
    assert df_from_block["data_none"].iloc[0] is None


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
