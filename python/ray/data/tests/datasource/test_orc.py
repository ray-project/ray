import os

import pandas as pd
import pyarrow as pa
import pytest
from pyarrow import orc

import ray
from ray.data._internal.arrow_block import _BATCH_SIZE_PRESERVING_STUB_COL_NAME
from ray.data._internal.datasource.orc_datasink import ORCDatasink
from ray.data._internal.util import rows_same
from ray.data.block import BlockAccessor


def _write_orc(path, table):
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)


def _list_visible_files(directory):
    return sorted(
        filename for filename in os.listdir(directory) if not filename.startswith(".")
    )


def _read_orc_dir(directory):
    return pa.concat_tables(
        orc.read_table(os.path.join(directory, filename))
        for filename in _list_visible_files(directory)
    )


def test_read_orc_basic(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    table = pa.table({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    _write_orc(path, table)

    ds = ray.data.read_orc(path)

    assert ds.count() == 3
    assert set(ds.schema().names) == {"id", "name"}
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_multiple_files(ray_start_regular_shared, tmp_path):
    for i in range(3):
        _write_orc(os.path.join(tmp_path, f"part_{i}.orc"), pa.table({"id": [i]}))

    ds = ray.data.read_orc(str(tmp_path))

    assert ds.count() == 3
    assert sorted(row["id"] for row in ds.take_all()) == [0, 1, 2]


def test_read_orc_include_paths(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": [0]}))

    ds = ray.data.read_orc(path, include_paths=True)

    rows = ds.take_all()
    assert all("path" in row for row in rows)
    assert all(row["path"].endswith("data.orc") for row in rows)


def test_read_orc_ignore_missing_paths(ray_start_regular_shared, tmp_path):
    existing = os.path.join(tmp_path, "data.orc")
    _write_orc(existing, pa.table({"id": [0, 1]}))
    missing = os.path.join(tmp_path, "does_not_exist.orc")

    ds = ray.data.read_orc([existing, missing], ignore_missing_paths=True)
    assert ds.count() == 2

    with pytest.raises(FileNotFoundError):
        ray.data.read_orc([existing, missing], ignore_missing_paths=False).materialize()


def test_read_orc_file_extensions_filtering(ray_start_regular_shared, tmp_path):
    _write_orc(os.path.join(tmp_path, "data.orc"), pa.table({"id": [0, 1]}))
    # A non-ORC file in the same directory should be filtered out by default.
    with open(os.path.join(tmp_path, "_SUCCESS"), "w") as f:
        f.write("")

    ds = ray.data.read_orc(str(tmp_path))
    assert ds.count() == 2

    # A directory with no matching files raises a clear error.
    empty_dir = os.path.join(tmp_path, "empty")
    os.makedirs(empty_dir)
    with open(os.path.join(empty_dir, "_SUCCESS"), "w") as f:
        f.write("")
    with pytest.raises(ValueError):
        ray.data.read_orc(empty_dir)


def test_read_orc_override_num_blocks(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "data.orc")
    _write_orc(path, pa.table({"id": list(range(100))}))

    ds = ray.data.read_orc(path, override_num_blocks=1)

    assert ds.count() == 100
    assert ds.materialize().num_blocks() == 1


def test_read_orc_partitioned(ray_start_regular_shared, tmp_path):
    from ray.data.datasource.partitioning import Partitioning, PartitionStyle

    os.makedirs(os.path.join(tmp_path, "year=2024"))
    _write_orc(
        os.path.join(tmp_path, "year=2024", "data.orc"),
        pa.table({"data": [0, 1]}),
    )

    ds = ray.data.read_orc(
        str(tmp_path), partitioning=Partitioning(PartitionStyle.HIVE)
    )

    rows = ds.take_all()
    assert sorted(row["data"] for row in rows) == [0, 1]
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_partitioned_with_partition_filter(ray_start_regular_shared, tmp_path):
    from ray.data.datasource.partitioning import (
        Partitioning,
        PartitionStyle,
        PathPartitionFilter,
    )

    for year in ("2023", "2024"):
        os.makedirs(os.path.join(tmp_path, f"year={year}"))
        _write_orc(
            os.path.join(tmp_path, f"year={year}", "data.orc"),
            pa.table({"data": [0, 1]}),
        )

    partition_filter = PathPartitionFilter.of(
        filter_fn=lambda partitions: partitions["year"] == "2024",
        style=PartitionStyle.HIVE,
    )
    ds = ray.data.read_orc(
        str(tmp_path),
        partitioning=Partitioning(PartitionStyle.HIVE),
        partition_filter=partition_filter,
    )

    rows = ds.take_all()
    assert len(rows) == 2
    assert all(row["year"] == "2024" for row in rows)


def test_read_orc_multiple_stripes(ray_start_regular_shared, tmp_path):

    path = os.path.join(tmp_path, "multi.orc")
    table = pa.table({"id": list(range(10000))})
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink, stripe_size=64 * 1024)

    ds = ray.data.read_orc(path)

    assert ds.count() == 10000
    assert sorted(row["id"] for row in ds.take_all()) == list(range(10000))


def test_read_orc_empty_file(ray_start_regular_shared, tmp_path):
    path = os.path.join(tmp_path, "empty.orc")
    table = pa.table(
        {
            "id": pa.array([], type=pa.int64()),
            "name": pa.array([], type=pa.string()),
        }
    )
    with pa.OSFile(path, "wb") as sink:
        orc.write_table(table, sink)

    ds = ray.data.read_orc(path)

    assert ds.count() == 0
    assert ds.take_all() == []


def test_orc_write(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0, 1, 2], "name": ["a", "b", "c"]})
    ds = ray.data.from_blocks([input_df])

    ds.write_orc(tmp_path)

    output_df = _read_orc_dir(tmp_path).to_pandas()
    assert rows_same(input_df, output_df)


@pytest.mark.parametrize("override_num_blocks", [None, 2])
def test_orc_roundtrip(ray_start_regular_shared, tmp_path, override_num_blocks):
    df = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})

    ds = ray.data.from_pandas([df], override_num_blocks=override_num_blocks)
    ds.write_orc(tmp_path)

    ds2 = ray.data.read_orc(str(tmp_path))
    ds2df = ds2.to_pandas()
    assert rows_same(ds2df, df)
    for entry in ds2._execute().blocks:
        assert (
            # pyrefly: ignore[no-matching-overload]
            BlockAccessor.for_block(ray.get(entry.ref)).size_bytes()
            == entry.metadata.size_bytes
        )


def test_orc_write_rejects_stream_compression(tmp_path):
    with pytest.raises(ValueError, match="compression="):
        ORCDatasink(str(tmp_path), open_stream_args={"compression": "gzip"})


def test_orc_write_rejects_zero_user_columns(tmp_path):
    block = BlockAccessor.for_block(
        pa.table({_BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(3)})
    )
    datasink = ORCDatasink(str(tmp_path))

    with pa.OSFile(os.path.join(tmp_path, "data.orc"), "wb") as file:
        with pytest.raises(ValueError, match="at least one column"):
            datasink.write_block_to_file(block, file)


def test_orc_write_strips_internal_columns(tmp_path):
    block = BlockAccessor.for_block(
        pa.table(
            {
                _BATCH_SIZE_PRESERVING_STUB_COL_NAME: pa.nulls(3),
                "id": [1, 2, 3],
            }
        )
    )
    output_path = os.path.join(tmp_path, "data.orc")
    datasink = ORCDatasink(str(tmp_path))

    with pa.OSFile(output_path, "wb") as file:
        datasink.write_block_to_file(block, file)

    output = orc.read_table(output_path)
    assert output.schema.names == ["id"]
    assert output.column("id").to_pylist() == [1, 2, 3]


def test_orc_write_compression(ray_start_regular_shared, tmp_path):
    input_df = pd.DataFrame({"id": [0, 1, 2]})
    ds = ray.data.from_blocks([input_df])

    ds.write_orc(tmp_path, compression="zstd")

    filenames = _list_visible_files(tmp_path)
    assert len(filenames) == 1
    output_file = os.path.join(tmp_path, filenames[0])
    assert orc.ORCFile(output_file).compression == "ZSTD"
    output_df = orc.read_table(output_file).to_pandas()
    assert rows_same(input_df, output_df)


def test_orc_write_args_fn_overrides_args(ray_start_regular_shared, tmp_path):
    ds = ray.data.range(3)

    ds.write_orc(
        tmp_path,
        arrow_orc_args_fn=lambda: {"compression": "zstd"},
        compression="uncompressed",
    )

    filenames = _list_visible_files(tmp_path)
    assert filenames
    assert all(
        orc.ORCFile(os.path.join(tmp_path, filename)).compression == "ZSTD"
        for filename in filenames
    )


def test_orc_write_empty(ray_start_regular_shared, tmp_path):
    df = pd.DataFrame({"id": pd.Series([], dtype="int64")})
    ds = ray.data.from_pandas(df)

    ds.write_orc(tmp_path)

    assert _list_visible_files(tmp_path) == []


@pytest.mark.parametrize("min_rows_per_file", [5, 10, 50])
def test_orc_write_min_rows_per_file(
    tmp_path, ray_start_regular_shared, min_rows_per_file
):
    ray.data.range(100, override_num_blocks=20).write_orc(
        tmp_path, min_rows_per_file=min_rows_per_file
    )

    filenames = _list_visible_files(tmp_path)
    assert len(filenames) == 100 // min_rows_per_file
    for filename in filenames:
        num_rows_written = orc.read_table(os.path.join(tmp_path, filename)).num_rows
        assert num_rows_written == min_rows_per_file


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
