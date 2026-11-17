import os
from typing import Any, Dict

import pyarrow
import pytest
from pyarrow.fs import LocalFileSystem

import ray
from ray.data.block import BlockAccessor
from ray.data.datasource import BlockBasedFileDatasink, RowBasedFileDatasink


@pytest.mark.parametrize("kind", ["row", "block", "parquet"])
def test_file_datasink_forwards_storage_options(monkeypatch, kind):
    from unittest.mock import Mock

    from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
    from ray.data.datasource import file_datasink

    filesystem = LocalFileSystem()
    resolver = Mock(return_value=(["output"], filesystem))
    monkeypatch.setattr(file_datasink, "_resolve_paths_and_filesystem", resolver)
    classes = {
        "row": file_datasink.RowBasedFileDatasink,
        "block": file_datasink.BlockBasedFileDatasink,
        "parquet": ParquetDatasink,
    }
    options = {"region": "us-west-2", "connect_timeout": 5.0}
    original = options.copy()
    sink = classes[kind]("s3://bucket/output", storage_options=options)
    resolver.assert_called_once_with(
        "s3://bucket/output", None, storage_options=options
    )
    assert sink.path == "output"
    assert options == original


@pytest.mark.parametrize(
    "api, constructor",
    [
        ("write_parquet", "ParquetDatasink"),
        ("write_json", "JSONDatasink"),
        ("write_images", "ImageDatasink"),
        ("write_csv", "CSVDatasink"),
        ("write_orc", "ORCDatasink"),
        ("write_tfrecords", "TFRecordDatasink"),
        ("write_webdataset", "WebDatasetDatasink"),
        ("write_numpy", "NumpyDatasink"),
    ],
)
def test_file_write_api_forwards_storage_options(
    monkeypatch, ray_start_regular_shared, api, constructor
):
    from unittest.mock import Mock

    import ray.data.dataset as dataset_module

    ds = ray.data.range(1)
    factory = Mock()
    write = Mock()
    monkeypatch.setattr(dataset_module, constructor, factory)
    monkeypatch.setattr(ds, "write_datasink", write)
    options = {"connect_timeout": 5.0}
    kwargs = {"column": "id"} if api in ("write_images", "write_numpy") else {}
    getattr(ds, api)("s3://bucket/output", storage_options=options, **kwargs)
    assert factory.call_args.kwargs["storage_options"] is options
    write.assert_called_once()


def test_parquet_write_storage_options_rejects_catalog(ray_start_regular_shared):
    ds = ray.data.range(1)
    with pytest.raises(ValueError, match="cannot be combined"):
        ds.write_parquet(
            "s3://bucket/output",
            storage_options={},
            catalog=object(),
            try_create_dir=False,
        )


@pytest.mark.parametrize(
    "path, filesystem, error",
    [
        ("s3://bucket/output", LocalFileSystem(), "cannot be combined"),
        ("/tmp/output", None, "requires s3://"),
    ],
)
def test_file_datasink_rejects_invalid_storage_options_combinations(
    path, filesystem, error
):
    from ray.data.datasource.file_datasink import _FileDatasink

    with pytest.raises(ValueError, match=error):
        _FileDatasink(path, filesystem=filesystem, storage_options={})


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

    ds.write_datasink(MockFileDatasink(path=path))

    assert os.path.isdir(path)


def test_write_creates_dir(tmp_path, ray_start_regular_shared):
    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            file.write(b"")

    ds = ray.data.range(1)
    path = os.path.join(tmp_path, "test")

    ds.write_datasink(MockFileDatasink(path=path, try_create_dir=True))

    assert os.path.isdir(path)


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_write_min_rows_per_file(tmp_path, ray_start_regular_shared, min_rows_per_file):
    class MockFileDatasink(BlockBasedFileDatasink):
        def write_block_to_file(self, block: BlockAccessor, file: "pyarrow.NativeFile"):
            for _ in range(block.num_rows()):
                file.write(b"row\n")

    ds = ray.data.range(100, override_num_blocks=20)

    ds.write_datasink(
        MockFileDatasink(path=tmp_path, min_rows_per_file=min_rows_per_file)
    )

    num_rows_written_total = 0
    for filename in os.listdir(tmp_path):
        with open(os.path.join(tmp_path, filename), "r") as file:
            num_rows_written = len(file.read().splitlines())
            assert num_rows_written == min_rows_per_file
            num_rows_written_total += num_rows_written

    assert num_rows_written_total == 100


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
