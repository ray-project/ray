import os
from typing import Any, Dict

import pyarrow
import pytest
from pyarrow.fs import LocalFileSystem

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import BlockBasedFileDatasink, RowBasedFileDatasink


class FlakyOutputStream:
    def __init__(self, stream: pyarrow.NativeFile, num_attempts: int):
        self._stream = stream
        self._num_attempts = num_attempts

    def __enter__(self):
        return self._stream.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        if self._num_attempts < 2:
            raise RuntimeError("AWS Error NETWORK_CONNECTION")

        self._stream.__exit__(exc_type, exc_value, traceback)


def test_flaky_block_based_open_output_stream(ray_start_regular_shared, tmp_path):
    class FlakyCSVDatasink(BlockBasedFileDatasink):
        def __init__(self, path: str):
            super().__init__(path)
            self._num_attempts = 0
            self._filesystem = LocalFileSystem()

        def open_output_stream(self, path: str) -> "pyarrow.NativeFile":
            stream = self._filesystem.open_output_stream(path)
            flaky_stream = FlakyOutputStream(stream, self._num_attempts)
            self._num_attempts += 1
            return flaky_stream

        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            block.to_pandas().to_csv(file)

    ds = ray.data.range(100)

    ds.write_datasink(FlakyCSVDatasink(tmp_path))

    expected_values = list(range(100))
    written_values = [row["id"] for row in ray.data.read_csv(tmp_path).take_all()]
    assert sorted(written_values) == sorted(expected_values)


def test_flaky_row_based_open_output_stream(ray_start_regular_shared, tmp_path):
    class FlakyTextDatasink(RowBasedFileDatasink):
        def __init__(self, path: str):
            super().__init__(path)
            self._num_attempts = 0
            self._filesystem = LocalFileSystem()

        def open_output_stream(self, path: str) -> "pyarrow.NativeFile":
            stream = self._filesystem.open_output_stream(path)
            flaky_stream = FlakyOutputStream(stream, self._num_attempts)
            self._num_attempts += 1
            return flaky_stream

        def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
            file.write(f"{row['id']}".encode())

    ds = ray.data.range(100)

    ds.write_datasink(FlakyTextDatasink(tmp_path))

    expected_values = [str(i) for i in range(100)]
    written_values = [row["text"] for row in ray.data.read_text(tmp_path).take_all()]
    assert sorted(written_values) == sorted(expected_values)


def test_flaky_write_block_to_file(ray_start_regular_shared, tmp_path):
    class FlakyCSVDatasink(BlockBasedFileDatasink):
        def __init__(self, path: str):
            super().__init__(path)
            self._num_attempts = 0

        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            if self._num_attempts < 2:
                self._num_attempts += 1
                raise RuntimeError("AWS Error INTERNAL_FAILURE")

            block.to_pandas().to_csv(file)

    ds = ray.data.range(100)

    ds.write_datasink(FlakyCSVDatasink(tmp_path))

    expected_values = list(range(100))
    written_values = [row["id"] for row in ray.data.read_csv(tmp_path).take_all()]
    assert sorted(written_values) == sorted(expected_values)


def test_flaky_write_row_to_file(ray_start_regular_shared, tmp_path):
    class FlakyTextDatasink(RowBasedFileDatasink):
        def __init__(self, path: str):
            super().__init__(path)
            self._num_attempts = 0

        def write_row_to_file(self, row: Dict[str, Any], file: "pyarrow.NativeFile"):
            if self._num_attempts < 2:
                self._num_attempts += 1
                raise RuntimeError("AWS Error INTERNAL_FAILURE")

            file.write(f"{row['id']}".encode())

    ds = ray.data.range(100)

    ds.write_datasink(FlakyTextDatasink(tmp_path))

    expected_values = [str(i) for i in range(100)]
    written_values = [row["text"] for row in ray.data.read_text(tmp_path).take_all()]
    assert sorted(written_values) == sorted(expected_values)


@pytest.mark.parametrize("num_rows", [0, 1])
def test_write_preserves_user_directory(num_rows, tmp_path, ray_start_regular_shared):
    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            file.write(b"")

    ds = ray.data.range(num_rows)
    path = os.path.join(tmp_path, "test")
    os.mkdir(path)  # User-created directory

    ds.write_datasource(MockFileDatasink(path=path))

    assert os.path.isdir(path)


def test_write_creates_dir(tmp_path, ray_start_regular_shared):
    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            file.write(b"")

    ds = ray.data.range(1)
    path = os.path.join(tmp_path, "test")

    ds.write_datasource(MockFileDatasink(path=path, try_create_dir=True))

    assert os.path.isdir(path)


@pytest.mark.parametrize("num_rows_per_file", [5, 10, 50])
def test_write_num_rows_per_file(tmp_path, ray_start_regular_shared, num_rows_per_file):
    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            for _ in range(block.num_rows()):
                file.write(b"row\n")

    ds = ray.data.range(100, parallelism=20)

    ds.write_datasink(
        MockFileDatasink(path=tmp_path, num_rows_per_file=num_rows_per_file)
    )

    num_rows_written_total = 0
    for filename in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, filename), "r") as file:
            num_rows_written = len(file.read().splitlines())
            assert num_rows_written == num_rows_per_file
            num_rows_written_total += num_rows_written

    assert num_rows_written_total == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
