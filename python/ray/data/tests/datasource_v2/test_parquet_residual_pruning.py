"""Compare full row results and actual worker scans, through the existing Arrow pruning path."""

import json
import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.datatype import DataType
from ray.data.expressions import UDFExpr, col, monotonically_increasing_id


@pytest.fixture(scope="module")
def ray_with_scan_observer():
    def install_observer():
        from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
            ParquetFileReader,
        )

        original = ParquetFileReader._iter_fragment_tables

        def observed(self, fragment, scanner_kwargs):
            log_dir = DataContext.get_current().get_config("test_scan_log_dir")
            with (log_dir / f"{os.getpid()}.jsonl").open("a") as log:
                log.write(
                    json.dumps([fragment.path, [g.id for g in fragment.row_groups]])
                    + "\n"
                )
            yield from original(self, fragment, scanner_kwargs)

        ParquetFileReader._iter_fragment_tables = observed

    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={"worker_process_setup_hook": install_observer},
    )
    try:
        yield
    finally:
        ray.shutdown()


@pytest.fixture
def scanned_groups(tmp_path, monkeypatch, ray_with_scan_observer, restore_data_context):
    log_dir = tmp_path / "scans"
    log_dir.mkdir()
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    context = DataContext.get_current()
    context.use_datasource_v2 = True
    # Each read task carries its test's directory, even when workers are reused.
    context.set_config("test_scan_log_dir", log_dir)

    def recorded():
        groups = []
        for path in log_dir.glob("*.jsonl"):
            assert int(path.stem) != os.getpid()
            for line in path.read_text().splitlines():
                file, ids = json.loads(line)
                groups.extend((file, i) for i in ids)
            path.unlink()
        assert len(groups) == len(set(groups)), "Unexpected repeated scan"
        return set(groups)

    return recorded


@pytest.fixture
def partitioned_data(tmp_path):
    """Two partitions with four groups each; directory values override stored p."""
    root = tmp_path / "data"
    tables = []
    for i, p in enumerate(("a", "b")):
        path = root / f"p={p}" / "data.parquet"
        path.parent.mkdir(parents=True)
        table = pa.table(
            {
                "id": range(i * 100, i * 100 + 40),
                "x": pa.array(range(40), pa.int8()),
                # Present directory values override this; absent keys must not.
                "p": ["b"] * 40,
                "blob": [None, b"\x00\xff"] * 20,
                "time": pa.array([None, 123456789] * 20, pa.timestamp("us")),
            }
        )
        pq.write_table(table, path, row_group_size=10)
        tables.append(table.set_column(2, "p", pa.array([p] * 40)))
    return root, pa.concat_tables(tables)


def _read_partitioned(root, schema, **kwargs):
    return ray.data.read_parquet(
        str(root),
        schema=schema,
        partitioning=Partitioning(
            PartitionStyle.HIVE,
            base_dir=str(root),
            field_names=["p"],
            null_fallback="__NULL__",
        ),
        **kwargs,
    )


@pytest.mark.parametrize(
    "predicate,pushed_predicate,groups",
    [
        ((col("p") == "a") | (col("x") >= 25), None, 6),
        ((col("p") == "a") | (col("x") >= 25), col("x") >= 15, 5),
        ((col("p") == "absent") | (col("x") < 0), None, 0),
    ],
    ids=["mixed_or", "pushed_filter", "empty"],
)
def test_mixed_predicates(
    partitioned_data, scanned_groups, predicate, pushed_predicate, groups
):
    root, table = partitioned_data
    ds = _read_partitioned(root, table.schema)
    if pushed_predicate is not None:
        ds = ds.filter(expr=pushed_predicate)
        table = ArrowBlockAccessor(table).filter(pushed_predicate)
    expected = ArrowBlockAccessor(table).filter(predicate).to_pylist()
    actual = (
        ds.filter(expr=predicate)
        .map_batches(lambda batch: batch, batch_format="pyarrow")
        .take_all()
    )
    assert sorted(actual, key=lambda row: row["id"]) == expected
    assert len(scanned_groups()) == groups


@pytest.mark.parametrize(
    "directory,partition_value,predicate",
    [
        ("p=__NULL__", None, col("p").is_null() | (col("x") >= 25)),
        ("unkeyed", "b", (col("p") == "b") | (col("x") >= 25)),
    ],
    ids=["null_partition", "missing_key_uses_physical_column"],
)
def test_partition_values(
    partitioned_data, scanned_groups, directory, partition_value, predicate
):
    root, table = partitioned_data
    (root / "p=b").rename(root / directory)
    oracle = table.set_column(2, "p", pa.array(["a"] * 40 + [partition_value] * 40))
    expected = ArrowBlockAccessor(oracle).filter(predicate).to_pylist()

    actual = _read_partitioned(root, table.schema).filter(expr=predicate).take_all()
    assert sorted(actual, key=lambda row: row["id"]) == expected
    assert len(scanned_groups()) == 6


@pytest.mark.parametrize(
    "filter_column,expected_ids",
    [("path", [0, 1]), ("id", [1])],
    ids=["stored_path", "unrelated_synthesized_column"],
)
def test_synthesized_columns(tmp_path, scanned_groups, filter_column, expected_ids):
    folder = tmp_path / "year=2024"
    folder.mkdir()
    file = folder / "data.parquet"
    table = pa.table({"id": [0, 1], "path": ["stored", "stored"]})
    pq.write_table(table, file, row_group_size=1)

    ds = ray.data.read_parquet(
        str(tmp_path),
        include_paths=True,
        partitioning=Partitioning(PartitionStyle.HIVE, field_names=["year"]),
    )
    predicate = (
        col(filter_column) == (str(file) if filter_column == "path" else 1)
    ) | (col("year") == "1999")
    expected = [{"id": i, "path": str(file), "year": "2024"} for i in expected_ids]
    assert (
        sorted(ds.filter(expr=predicate).take_all(), key=lambda row: row["id"])
        == expected
    )
    assert len(scanned_groups()) == len(expected_ids)


def test_block_udf_disables_pruning(partitioned_data, scanned_groups):
    root, table = partitioned_data
    predicate = (col("p") == "a") | (col("x") >= 25)
    expected = table.to_pylist()
    for row in expected:
        row["x"] += 100

    def transform(table):
        return table.set_column(
            table.schema.get_field_index("x"), "x", pc.add(table["x"], 100)
        )

    ds = _read_partitioned(root, table.schema, _block_udf=transform)
    assert (
        sorted(ds.filter(expr=predicate).take_all(), key=lambda row: row["id"])
        == expected
    )
    assert len(scanned_groups()) == 8


def test_limit_before_filter_disables_pruning(
    partitioned_data, scanned_groups, monkeypatch
):
    root, table = partitioned_data
    # A filter above a limit must not select later rows to fill that limit.
    monkeypatch.setattr(
        DataContext.get_current().execution_options, "preserve_order", True
    )
    ds = _read_partitioned(root / "p=b", table.schema).limit(10)
    assert ds.filter(expr=(col("p") == "absent") | (col("x") >= 25)).take_all() == []
    assert len(scanned_groups()) == 4


@pytest.mark.parametrize(
    "values,predicate,row_group_size,expected_ids,schema",
    [
        (pa.array([35.0, 40.0, float("nan")]), ~(col("x") >= 30.0), 3, [2], None),
        (pa.array([100, 1], pa.int8()), (col("x") + 100) > 120, 1, [0], None),
        (
            pa.array([datetime(2020, 1, 1, 12), datetime(2020, 1, 2)]),
            col("x") == date(2020, 1, 1),
            2,
            [0],
            pa.schema([("id", pa.int64()), ("x", pa.date32()), ("p", pa.string())]),
        ),
    ],
    ids=["nan_negation", "int8_arithmetic", "reader_cast"],
)
def test_residual_preserves_row_evaluation(
    tmp_path, scanned_groups, values, predicate, row_group_size, expected_ids, schema
):
    folder = tmp_path / "p=other"
    folder.mkdir()
    file = folder / "data.parquet"
    pq.write_table(
        pa.table({"id": range(len(values)), "x": values}),
        file,
        row_group_size=row_group_size,
    )
    rows = (
        ray.data.read_parquet(str(tmp_path), schema=schema)
        .filter(expr=(col("p") == "keep") | predicate)
        .take_all()
    )
    assert sorted(row["id"] for row in rows) == expected_ids
    assert scanned_groups() == {
        (str(file), i) for i in range(len(values) // row_group_size)
    }


@pytest.mark.parametrize(
    "extra_predicate,include_paths,expected_ids",
    [
        (col("x") != 40.0, False, [13, 15, 17, 19]),
        (col("path") != "nope", True, list(range(12, 20))),
        (
            UDFExpr(
                fn=lambda values: pc.greater(values, 7),
                args=[col("y")],
                kwargs={},
                data_type=DataType.from_arrow(pa.bool_()),
            ),
            False,
            [16, 17, 18, 19],
        ),
    ],
    ids=["unsafe_float", "synthesized_path", "udf"],
)
def test_safe_conjunct_still_prunes(
    tmp_path, scanned_groups, extra_predicate, include_paths, expected_ids
):
    file = tmp_path / "p=other" / "data.parquet"
    file.parent.mkdir()
    pq.write_table(
        pa.table(
            {
                "id": range(20),
                "y": [i // 2 for i in range(20)],
                "x": [40.0, float("nan")] * 10,
            }
        ),
        file,
        row_group_size=2,
    )
    predicate = ((col("p") == "keep") | (col("y") > 5)) & extra_predicate
    rows = (
        ray.data.read_parquet(str(tmp_path), include_paths=include_paths)
        .filter(expr=predicate)
        .take_all()
    )
    assert sorted(row["id"] for row in rows) == expected_ids
    assert scanned_groups() == {(str(file), i) for i in range(6, 10)}


def test_non_idempotent_residual_keeps_row_positions(tmp_path, scanned_groups):
    file = tmp_path / "p=other" / "data.parquet"
    file.parent.mkdir()
    pq.write_table(pa.table({"y": range(10)}), file, row_group_size=1)
    predicate = ((col("p") == "keep") | (col("y") > 5)) & (
        monotonically_increasing_id() >= 6
    )
    # This small file fits in one read task, so IDs match the row positions.
    rows = ray.data.read_parquet(str(tmp_path)).filter(expr=predicate).take_all()
    assert sorted(row["y"] for row in rows) == [6, 7, 8, 9]
    assert scanned_groups() == {(str(file), i) for i in range(10)}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
