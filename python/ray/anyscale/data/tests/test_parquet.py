import functools
import re

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import FileSystemHandler, LocalFileSystem, PyFileSystem

import ray
from ray.data.tests.conftest import *  # noqa


def flaky(func):
    """Error on the first call, then succeed on the second."""
    has_errored = False

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        nonlocal has_errored
        if not has_errored:
            has_errored = True
            raise Exception("Transient error")
        else:
            return func(*args, **kwargs)

    return wrapper


# TODO(@bveeramani): Rather than calling `call_with_retry` whenever we invoke filesystem
# methods, we could create a filesystem wrapper that retries on transient errors. The
# implementation could be similar to the `FlakyFileSystemHandler` class below.
class FlakyFileSystemHandler(FileSystemHandler):
    def __init__(self, fs):
        self._fs = fs

    @flaky
    def copy_file(self, src, dest):
        self._fs.copy_file(src, dest)

    @flaky
    def create_dir(self, path, recursive):
        self._fs.create_dir(path, recursive=recursive)

    @flaky
    def delete_dir(self, path):
        self._fs.delete_dir(path)

    @flaky
    def delete_dir_contents(self, path, missing_dir_ok=False):
        self._fs.delete_dir_contents(path, missing_dir_ok=missing_dir_ok)

    @flaky
    def delete_file(self, path):
        self._fs.delete_file(path)

    @flaky
    def delete_root_dir_contents(self, path):
        self._fs._delete_dir_contents("/", accept_root_dir=True)

    @flaky
    def get_file_info(self, paths):
        return self._fs.get_file_info(paths)

    @flaky
    def get_file_info_selector(self, selector):
        return self._fs.get_file_info(selector)

    # Don't use the flaky decorator for 'get_type_name' because it presumably doesn't
    # use I/O.
    def get_type_name(self):
        return self._fs.type_name

    @flaky
    def move(self, src, dest):
        return self._fs.move(src, dest)

    # Don't use the flaky decorator for 'normalize_path' because it presumably doesn't
    # use I/O.
    def normalize_path(self, path):
        return self._fs.normalize_path(path)

    @flaky
    def open_append_stream(self, path, metadata):
        return self._fs.open_append_stream(path, metadata=metadata)

    @flaky
    def open_input_file(self, path):
        return self._fs.open_input_file(path)

    @flaky
    def open_input_stream(self, path):
        return self._fs.open_input_stream(path)

    @flaky
    def open_output_stream(self, path, metadata):
        return self._fs.open_output_stream(path, metadata=metadata)


def test_transient_error_handling(restore_data_context, ray_start_regular_shared):
    ctx = ray.data.DataContext.get_current()
    ctx.retried_io_errors.append("Transient error")
    # 'FlakyFileSystemHandler' raises an error on the first call to any filesystem
    # method, then succeeds on the second call.
    fs = PyFileSystem(FlakyFileSystemHandler(LocalFileSystem()))

    ray.data.read_parquet("example://iris.parquet", filesystem=fs).materialize()


@pytest.mark.parametrize(
    "columns,expected_exception,expected_message",
    [
        ([], ValueError, "`columns` cannot be an empty list."),
        ("not_a_list", TypeError, "`columns` must be a list of strings."),
        (["valid_col", 123], TypeError, "All elements in `columns` must be strings."),
        (["variety", "sepal.length"], None, None),
    ],
)
def test_empty_columns_with_read_parquet(
    ray_start_regular_shared, columns, expected_exception, expected_message
):
    if expected_exception:
        with pytest.raises(expected_exception, match=expected_message):
            ray.data.read_parquet(
                "example://iris.parquet", columns=columns
            ).materialize()
    else:
        try:
            schema = ray.data.read_parquet(
                "example://iris.parquet", columns=columns
            ).schema()
            assert schema.names == ["variety", "sepal.length"]
        except Exception as e:
            pytest.fail(f"Unexpected exception raised: {e}")


def test_read_parquet_produces_target_size_blocks(
    ray_start_regular_shared, tmp_path, restore_data_context
):
    table = pa.Table.from_pydict({"data": ["\0" * 1024 * 1024]})  # 1 MiB of data
    pq.write_table(table, tmp_path / "test1.parquet")
    pq.write_table(table, tmp_path / "test2.parquet")
    pq.write_table(table, tmp_path / "test3.parquet")
    pq.write_table(table, tmp_path / "test4.parquet")
    ray.data.DataContext.get_current().target_max_block_size = 2 * 1024 * 1024  # 2 MiB
    ds = ray.data.read_parquet(tmp_path)
    actual_block_sizes = [
        block_metadata.size_bytes
        for bundle in ds.iter_internal_ref_bundles()
        for block_metadata in bundle.metadata
    ]
    assert all(
        block_size == pytest.approx(2 * 1024 * 1024, rel=0.01)
        for block_size in actual_block_sizes
    ), actual_block_sizes


@pytest.mark.parametrize(
    "filter_expr, expected_row_count, expect_error, expected_error_message",
    [
        # Valid filter expression
        ("column03 == 0 and column04 == 0", 1, False, None),
        # Invalid filter expression (referencing partition columns which are in schema)
        (
            "column01 == 0 and column02 == 0",
            None,
            True,
            "RuntimeError: Filter expression: '((column01 == 0) and (column02 == 0))' failed on parquet file: '<parquet_file>' with columns: {'column03', 'column04'}",  # noqa: E501
        ),
        # Invalid filter expression (referencing non-partition column)
        (
            "non_existing_column == 0",
            None,
            True,
            "RuntimeError: Filter expression: '(non_existing_column == 0)' failed on parquet file: '<parquet_file>' with columns: {'column03', 'column04'}",  # noqa: E501
        ),
    ],
)
def test_read_parquet_filter_expr_partition_columns(
    ray_start_regular_shared,
    tmp_path,
    filter_expr,
    expected_row_count,
    expect_error,
    expected_error_message,
):
    """Verify handling of valid and invalid filter expressions on partitioned
    columns.
    """

    num_partitions = 10
    rows_per_partition = 10
    num_rows = num_partitions * rows_per_partition

    # DataFrame with partition columns
    df = pd.DataFrame(
        {
            "column01": list(range(num_partitions)) * rows_per_partition,
            "column02": list(range(num_partitions)) * rows_per_partition,
            "column03": list(range(num_rows)),
            "column04": list(range(num_rows)),
        }
    )

    # Write data to a partitioned Parquet dataset
    ds = ray.data.from_pandas(df)
    ds.write_parquet(tmp_path, partition_cols=["column01", "column02"])

    if expect_error:
        # Verify the exception type and message
        with pytest.raises(RuntimeError) as excinfo:
            ray.data.read_parquet(tmp_path).filter(expr=filter_expr).materialize()
        actual_message = str(excinfo.value)

        # Replace the parquet file name in the expected error message with a placeholder
        actual_message_core = re.sub(r"\s+", " ", actual_message.strip())
        expected_message_core = re.sub(r"\s+", " ", expected_error_message.strip())

        # Replace specific file names in the error messages with a placeholder
        actual_message_core = re.sub(
            r"parquet file: '[^']+'",
            "parquet file: '<parquet_file>'",
            actual_message_core,
        )
        expected_message_core = re.sub(
            r"parquet file: '[^']+'",
            "parquet file: '<parquet_file>'",
            expected_message_core,
        )

        # Sort the set in the message
        def normalize_set_order(message):
            return re.sub(
                r"{([^}]*)}",
                lambda m: "{" + ", ".join(sorted(m.group(1).split(", "))) + "}",
                message,
            )

        # Sort the schema columns set in the message before comparing
        actual_message_core = normalize_set_order(actual_message_core)
        expected_message_core = normalize_set_order(expected_message_core)

        assert expected_message_core in actual_message_core, (
            f"Expected error message to contain: '{expected_message_core}', "
            f"but got: '{actual_message_core}'"
        )
    else:
        # Verify the filtered dataset row count
        filtered_ds = ray.data.read_parquet(tmp_path).filter(expr=filter_expr)
        assert (
            filtered_ds.count() == expected_row_count
        ), f"Expected {expected_row_count} rows, but got {filtered_ds.count()} rows."


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
