"""Public-API residual pruning checks on a single-node Ray test cluster."""

import datetime
import json
import os
from typing import Any, Dict, List

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


@pytest.fixture
def scanned_groups(tmp_path, monkeypatch):
    log_dir = str(tmp_path / "scan-log")
    os.mkdir(log_dir)

    def install_observer():
        from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
            ParquetFileReader,
        )

        original = ParquetFileReader._iter_fragment_tables

        def observed(self, fragment, scanner_kwargs):
            # Observe scan requests, not footer prefetch or filesystem ranges.
            groups = fragment.row_groups
            assert groups is not None
            with open(os.path.join(log_dir, f"{os.getpid()}.jsonl"), "a") as log:
                log.write(json.dumps([fragment.path, [g.id for g in groups]]) + "\n")
            yield from original(self, fragment, scanner_kwargs)

        ParquetFileReader._iter_fragment_tables = observed

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "worker_process_setup_hook": install_observer,
        },
    )
    context = DataContext.get_current()
    previous = context.use_datasource_v2
    context.use_datasource_v2 = True

    def recorded():
        events = []
        for path in (tmp_path / "scan-log").glob("*.jsonl"):
            assert int(path.stem) != os.getpid(), "Observer ran only in the driver"
            events.extend(json.loads(line) for line in path.read_text().splitlines())
            path.unlink()  # Each query must supply its own worker evidence.
        return {(path, group) for path, groups in events for group in groups}

    try:
        yield recorded
    finally:
        context.use_datasource_v2 = previous
        ray.shutdown()


@pytest.mark.parametrize(
    "case",
    ["mixed", "q_and_r", "map", "count", "not", "in", "empty", "hive_null_marker"],
)
def test_residual_pruning_rows_and_worker_groups(
    tmp_path, monkeypatch, scanned_groups, case
):
    root = tmp_path / "data"
    all_rows: List[Dict[str, Any]] = []
    partitions = (
        "a",
        "__HIVE_DEFAULT_PARTITION__" if case == "hive_null_marker" else "b",
    )
    for index, p in enumerate(partitions):
        folder = root / f"p={p}"
        folder.mkdir(parents=True)
        table = pa.table(
            {
                "id": range(index * 100, index * 100 + 40),
                "x": range(40),
                "blob": [b"payload"] * 40,
                "timestamp": [datetime.datetime(2025, 1, 1)] * 40,
            }
        )
        pq.write_table(table, folder / "data.parquet", row_group_size=10)
        all_rows.extend({**row, "p": p} for row in table.to_pylist())
    residual = (col("p") == "a") | (col("x") >= 25)
    if case == "hive_null_marker":
        # Match Ray's existing reader: this marker is currently a string,
        # unlike Arrow Hive discovery. Do not silently change that contract.
        residual = col("p").is_null() | (col("x") >= 25)
    elif case == "not":
        residual = ~((col("p") == "a") & (col("x") < 25))
    elif case == "in":
        residual = col("p").is_in(["a"]) | col("x").is_in([5, 35])
    elif case == "empty":
        residual = ((col("p") == "a") & (col("x") > 100)) | (
            (col("p") == "b") & (col("x") < 0)
        )

    def matches(row):
        if case == "hive_null_marker":
            return row["p"] is None or row["x"] >= 25
        if case == "not":
            return not (row["p"] == "a" and row["x"] < 25)
        if case == "in":
            return row["p"] == "a" or row["x"] in (5, 35)
        if case == "empty":
            return False
        return row["p"] == "a" or row["x"] >= 25

    expected = [row for row in all_rows if matches(row)]
    if case == "q_and_r":
        expected = [row for row in expected if row["x"] >= 5]

    def query():
        ds = ray.data.read_parquet(
            str(root),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(root), field_names=["p"]
            ),
        )
        if case == "q_and_r":
            ds = ds.filter(expr=col("x") >= 5)
        ds = ds.filter(expr=residual)
        if case == "map":
            ds = ds.map_batches(lambda batch: batch, batch_format="pyarrow")
        return (
            ds.count()
            if case == "count"
            else sorted(ds.take_all(), key=lambda row: row["id"])
        )

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert actual == control == (len(expected) if case == "count" else expected)
    expected_groups = {"empty": 0, "hive_null_marker": 4}.get(case, 6)
    assert len(enabled) == expected_groups
    assert enabled < disabled
    assert len(disabled) == 8


def _run_with_and_without_hint(query, monkeypatch, scanned_groups):
    actual = query()
    enabled = scanned_groups()
    configure = FooterFileIndexer.with_residual_pruning
    with monkeypatch.context() as patch:
        patch.setattr(
            FooterFileIndexer,
            "with_residual_pruning",
            lambda self, *args, **kwargs: configure(self),
        )
        control = query()
    return actual, control, enabled, scanned_groups()


@pytest.mark.parametrize("case", ["nan", "signed_zero_in", "int8_add"])
def test_unsafe_residual_keeps_rows_and_scan_groups(
    tmp_path, monkeypatch, scanned_groups, case
):
    values = {
        "nan": [float("nan"), 0.0],
        "signed_zero_in": [-0.0, 0.0],
        "int8_add": [100, 1],
    }[case]
    predicate = ~(col("x") == 0.0)
    selected = [0]
    if case == "signed_zero_in":
        predicate = col("x").is_in([0.0])
        # Use eager Arrow semantics; preserve the sign of zero in row checks.
        selected = [
            i
            for i, keep in enumerate(
                pa.compute.is_in(
                    pa.array(values), value_set=pa.array([0.0])
                ).to_pylist()
            )
            if keep
        ]
    elif case == "int8_add":
        predicate = col("x") + 100 > 127
    residual = (col("p") == "keep") | predicate
    root = tmp_path / "unsafe"
    expected = []
    for index, partition in enumerate(("a", "b")):
        folder = root / f"p={partition}"
        folder.mkdir(parents=True)
        table = pa.table(
            {
                "id": [index * 100, index * 100 + 1],
                "x": pa.array(
                    values, type=pa.int8() if case == "int8_add" else pa.float64()
                ),
            }
        )
        pq.write_table(table, folder / "data.parquet", row_group_size=2)
        expected.extend({**table.to_pylist()[i], "p": partition} for i in selected)

    def query():
        return ray.data.read_parquet(str(root)).filter(expr=residual).take_all()

    def canonical(rows):
        return sorted(
            [
                {k: v.hex() if isinstance(v, float) else v for k, v in row.items()}
                for row in rows
            ],
            key=lambda row: row["id"],
        )

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert canonical(actual) == canonical(control) == canonical(expected)
    # A positive exact count rejects missing logs; equality alone would not.
    assert enabled == disabled
    assert len(enabled) == 2
