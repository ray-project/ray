"""Real-worker schema-only results and conservative error boundaries."""
import json
import os
from collections import Counter
from dataclasses import replace
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.logical.operators import ListFiles
from ray.data._internal.logical.rules.derive_list_files_pushdown import (
    DeriveListFilesPushdown,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _install_worker_fault(directory):
    from pathlib import Path

    from ray.data._internal.datasource_v2.common.file_reader import FileReader
    from ray.data._internal.util import RetryingPyFileSystemHandler

    native = FileReader._iter_unit_tables
    native_open = RetryingPyFileSystemHandler.open_input_file
    root = Path(directory)

    def claim(mode):
        config = root / "fault-mode"
        if not config.exists() or config.read_text() != mode:
            return False
        try:
            fd = os.open(
                str(root / "fault-claimed"), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
            )
        except FileExistsError:
            return False
        with os.fdopen(fd, "w") as handle:
            handle.write(str(os.getpid()))
        return True

    def read(self, fragment_to_read, scanner_kwargs):
        iterator = native(self, fragment_to_read, scanner_kwargs)
        try:
            for table in iterator:
                if claim("worker-exit"):
                    # Kill only this test's current Ray worker after a genuine
                    # native read, so Ray must replay the task on another worker.
                    os._exit(86)
                yield table
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()

    def open_input_file(self, path):
        if claim("missing-footer"):
            # The built-in handler keeps its admitted type; only this test's
            # synthetic source is removed before the real native open.
            Path(path).unlink()
        return native_open(self, path)

    FileReader._iter_unit_tables = read
    RetryingPyFileSystemHandler.open_input_file = open_input_file


@pytest.fixture
def execution(tmp_path, monkeypatch):
    audit = tmp_path / "audit"
    audit.mkdir()
    audit_path = str(audit)

    def worker_setup():
        from ray.data.tests.datasource_v2.test_listing_pruning import (
            _install_read_audit,
        )

        _install_read_audit(audit_path)
        module = __import__(
            "ray.data.tests.datasource_v2.test_schema_only_manifest",
            fromlist=["_install_worker_fault"],
        )
        module._install_worker_fault(audit_path)

    previous = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.execution_options.preserve_order = False
    DataContext._set_current(context)
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "2")
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_BATCH_SIZE", "2")
    state = {"enabled": False, "hint_seen": False}
    native = DeriveListFilesPushdown.apply

    def apply(self, plan):
        configured = native(self, plan)
        if state["enabled"]:
            state["hint_seen"] |= any(
                getattr(n.file_indexer, "_listing_hint", None) is not None
                for n in configured.dag.post_order_iter()
                if isinstance(n, ListFiles)
            )
            return configured

        def clear(node):
            if isinstance(node, ListFiles):
                return replace(
                    node, file_indexer=node.file_indexer.without_residual_pruning()
                )
            return node

        return type(configured)(
            configured.dag._apply_transform(clear), configured.context
        )

    monkeypatch.setattr(DeriveListFilesPushdown, "apply", apply)
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        log_to_driver=False,
        object_store_memory=256 * 1024 * 1024,
        runtime_env={"worker_process_setup_hook": worker_setup},
    )
    try:
        yield state, audit
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


@pytest.mark.parametrize(
    "case",
    [
        "original",
        "all_r",
        "all_q_stats",
        "all_q_runtime",
        "partition_q",
        "int8",
        "projection",
    ],
)
def test_empty_output_schema_and_actual_reads(tmp_path, execution, case):
    state, audit = execution
    root = tmp_path / "data"
    paths = []
    expected_rows = Counter()
    if case == "original":
        for year in range(2016, 2024):
            for month in range(1, 5):
                path = root / f"year={year}" / f"month={month:02d}" / "part.parquet"
                path.parent.mkdir(parents=True)
                n = len(paths)
                pq.write_table(
                    pa.table({"x": range(n * 1000, n * 1000 + 1000)}),
                    path,
                    row_group_size=200,
                )
                paths.append(str(path))
                expected_rows.update(
                    (x, str(year), f"{month:02d}")
                    for x in range(n * 1000, n * 1000 + 1000)
                    if year == 2023 or x < 1000
                )
    else:
        path = root / "p=1" / "__empty__"
        path.parent.mkdir(parents=True)
        field = pa.field("x", pa.int64(), nullable=case != "int8")
        pq.write_table(
            pa.Table.from_arrays([pa.array([0, 2] * 5)], schema=pa.schema([field])),
            path,
            row_group_size=2,
        )
        paths.append(str(path))
    results = []
    for enabled in [False, True]:
        state["enabled"] = enabled
        (audit / "label").write_text(str(enabled))
        kwargs: dict[str, Any] = {}
        if case != "original":
            kwargs = {"file_extensions": None}
        if case == "int8":
            kwargs.update(
                schema=pa.schema(
                    [
                        pa.field("x", pa.int64(), nullable=False),
                        pa.field("p", pa.int8(), nullable=False),
                    ]
                ),
                partitioning=Partitioning(
                    PartitionStyle.HIVE,
                    base_dir=str(root),
                    field_names=["p"],
                    field_types={"p": int},
                ),
            )
        query = ray.data.read_parquet(str(root), **kwargs)
        residual = (
            (col("year") == "2023") | (col("x") < 1000)
            if case == "original"
            else (col("p") == (99 if case == "int8" else "absent")) | (col("x") < 0)
        )
        if case.startswith("all_q"):
            query = query.filter(
                expr=(col("x") < 0) if case == "all_q_stats" else (col("x") == 1)
            )
            residual = (col("p") == "1") | (col("x") > 100)
        if case == "partition_q":
            query = query.filter(expr=col("p") == "absent")
        query = query.filter(expr=residual)
        if case == "projection":
            query = query.select_columns(["p", "x"])
        result = query.materialize()
        rows = result.take_all()
        assert len(rows) == (5000 if case == "original" else 0)
        if case == "original":
            assert (
                Counter((row["x"], row["year"], row["month"]) for row in rows)
                == expected_rows
            )
        schema = result.schema(fetch_if_missing=False)
        frame = result.to_pandas()
        records = [
            json.loads(line)
            for p in audit.glob("*.jsonl")
            for line in p.read_text().splitlines()
        ]
        starts = [
            r for r in records if r["label"] == str(enabled) and r["event"] == "start"
        ]
        groups = sum(len(r["groups"]) for r in starts)
        if case == "original":
            assert groups == (25 if enabled else 160)
            assert len({r["path"] for r in starts}) == (5 if enabled else 32)
        elif case in ("all_r", "int8"):
            assert groups == (0 if enabled else 5)
        elif case in ("all_q_stats", "partition_q"):
            assert groups == 0
        elif case in ("all_q_runtime", "projection"):
            assert groups == 5
        assert all(r["pid"] != os.getpid() for r in starts)
        results.append((schema, list(frame.columns)))
        # A distinct label keeps this second consumption out of scan assertions.
        (audit / "label").write_text(f"direct-{enabled}")
        assert list(query.to_pandas().columns) == (
            ["x", "year", "month"]
            if case == "original"
            else ["p", "x"]
            if case == "projection"
            else ["x", "p"]
        )
    before, after = results
    if case == "partition_q" and after[0] is None:
        assert before[0] is None
        pytest.xfail("Partition Q removes all files before footer layout certification")
    assert after[0] is not None
    assert after[1] == (
        ["x", "year", "month"]
        if case == "original"
        else ["p", "x"]
        if case == "projection"
        else ["x", "p"]
    )
    if before[0] is not None:
        assert before[0].base_schema.equals(after[0].base_schema, check_metadata=True)
    if case != "projection":
        assert state["hint_seen"]


@pytest.mark.parametrize("case", ["q_eliminated", "q_kept", "r_error"])
def test_actual_filter_errors_are_not_hidden(tmp_path, execution, case):
    state, audit = execution
    path = tmp_path / "data" / "p=b" / "data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"x": [0, 2, 0, 2]}), path, row_group_size=2)
    for enabled in [False, True]:
        state["enabled"] = enabled
        (audit / "label").write_text(str(enabled))
        source = ray.data.read_parquet(str(path.parent.parent))
        if case == "r_error":
            query = source.filter(
                expr=(col("p") == "absent") | ((col("x") / (col("x") - col("x"))) > 0)
            )
        else:
            source = source.filter(expr=(col("x") / (col("x") - 0)) > 0)
            query = source.filter(
                expr=(col("p") == ("b" if case == "q_kept" else "absent"))
                | (col("x") < 0)
            )
        if enabled and case == "q_eliminated":
            result = query.materialize()
            assert result.take_all() == []
            assert result.schema() is not None
        else:
            with pytest.raises(pa.ArrowInvalid, match="divide by zero"):
                query.take_all()


@pytest.mark.parametrize("explicit_schema", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("keep", [False, True])
def test_valid_mixed_integer_files_preserve_order_independent_results(
    tmp_path, execution, reverse, keep, explicit_schema
):
    state, audit = execution
    root = tmp_path / "mixed" / "p=b"
    root.mkdir(parents=True)
    paths = []
    for name, kind in [("narrow", pa.int32()), ("wide", pa.int64())]:
        path = root / (name + ".parquet")
        pq.write_table(
            pa.table({"x": pa.array(range(10), type=kind)}), path, row_group_size=2
        )
        paths.append(str(path))
    if reverse:
        paths.reverse()
    schema = pa.schema([("x", pa.int64()), ("p", pa.string())])
    baseline_schema = None
    results = []
    for enabled in [False, True]:
        state["enabled"] = enabled
        (audit / "label").write_text(str(enabled))
        query = ray.data.read_parquet(
            paths, schema=schema if explicit_schema else None
        ).filter(
            expr=(col("p") == "absent") | ((col("x") >= 8) if keep else (col("x") < 0))
        )
        result = query.materialize()
        rows = result.take_all()
        assert sorted(row["x"] for row in rows) == ([8, 8, 9, 9] if keep else [])
        actual_schema = result.schema(fetch_if_missing=False)
        assert actual_schema is not None
        if explicit_schema:
            assert actual_schema.base_schema.equals(schema, check_metadata=True)
        if not enabled:
            baseline_schema = actual_schema.base_schema
        else:
            assert baseline_schema is not None
            assert actual_schema.base_schema.equals(
                baseline_schema, check_metadata=True
            )
        results.append(rows)
        starts = [
            json.loads(line)
            for p in audit.glob("*.jsonl")
            for line in p.read_text().splitlines()
            if json.loads(line)["label"] == str(enabled)
            and json.loads(line)["event"] == "start"
        ]
        by_name = {
            name: sum(
                len(record["groups"])
                for record in starts
                if record["path"].endswith(name + ".parquet")
            )
            for name in ["narrow", "wide"]
        }
        assert by_name == {"narrow": 5, "wide": (1 if keep else 0) if enabled else 5}
    assert state["hint_seen"]


def test_projection_empty_with_actual_listing_hint(tmp_path, execution):
    state, audit = execution
    root = tmp_path / "projected" / "p=b"
    root.mkdir(parents=True)
    pq.write_table(
        pa.table({"x": range(10), "unused": [True] * 10}),
        root / "data.parquet",
        row_group_size=2,
    )
    schemas = []
    for enabled in [False, True]:
        state["enabled"] = enabled
        (audit / "label").write_text(str(enabled))
        query = ray.data.read_parquet(str(root.parent), columns=["p", "x"]).filter(
            expr=(col("p") == "absent") | (col("x") < 0)
        )
        result = query.materialize()
        assert result.take_all() == []
        schema = result.schema(fetch_if_missing=False)
        assert schema is not None
        assert schema.names == ["p", "x"]
        assert list(result.to_pandas().columns) == ["p", "x"]
        schemas.append(schema.base_schema)
        starts = [
            json.loads(line)
            for p in audit.glob("*.jsonl")
            for line in p.read_text().splitlines()
            if json.loads(line)["label"] == str(enabled)
            and json.loads(line)["event"] == "start"
        ]
        assert sum(len(record["groups"]) for record in starts) == (0 if enabled else 5)
    assert schemas[0].equals(schemas[1], check_metadata=True)
    assert state["hint_seen"]


@pytest.mark.parametrize("fault", ["worker-exit", "missing-footer"])
def test_real_worker_failure_and_required_footer_error(tmp_path, execution, fault):
    state, audit = execution
    root = tmp_path / "fault" / "p=b"
    root.mkdir(parents=True)
    pq.write_table(pa.table({"x": range(10)}), root / "data.parquet", row_group_size=2)
    state["enabled"] = True
    (audit / "label").write_text(fault)
    # Build the public read before enabling the fault: driver schema discovery
    # succeeds, and only an actual footer/read worker is affected.
    query = ray.data.read_parquet(str(root.parent)).filter(
        expr=(col("p") == "absent") | (col("x") >= 8)
    )
    (audit / "fault-mode").write_text(fault)
    if fault == "missing-footer":
        with pytest.raises(Exception, match="No such file|not found|does not exist"):
            query.materialize()
    else:
        result = query.materialize()
        assert sorted(row["x"] for row in result.take_all()) == [8, 9]
        assert result.schema().names == ["x", "p"]
        crashed = int((audit / "fault-claimed").read_text())
        records = [
            json.loads(line)
            for p in audit.glob("*.jsonl")
            for line in p.read_text().splitlines()
        ]
        assert any(r["pid"] == crashed and r["event"] == "start" for r in records)
        assert any(r["pid"] != crashed and r["event"] == "complete" for r in records)
    assert int((audit / "fault-claimed").read_text()) != os.getpid()
    assert state["hint_seen"]


def test_seeded_row_group_shuffle_declines_residual_hint(tmp_path, execution):
    from ray.data._internal.logical.operators import Filter
    from ray.data._internal.logical.optimizers import LogicalOptimizer
    from ray.data.datasource.file_based_datasource import FileShuffleConfig

    state, audit = execution
    root = tmp_path / "shuffled"
    for partition, start in [("a", 0), ("b", 40)]:
        path = root / f"p={partition}" / "data.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(
            pa.table({"x": range(start, start + 40)}), path, row_group_size=10
        )
    state["enabled"] = True
    assert not DataContext.get_current().execution_options.preserve_order
    residual = (col("p") == "a") | (col("x") >= 70)

    # The same unshuffled query must be eligible, isolating the shuffle guard.
    ordinary = ray.data.read_parquet(str(root)).filter(expr=residual)
    ordinary_plan = LogicalOptimizer().optimize(ordinary._logical_plan)
    assert any(
        getattr(node.file_indexer, "_listing_hint", None) is not None
        for node in ordinary_plan.dag.post_order_iter()
        if isinstance(node, ListFiles)
    )
    state["hint_seen"] = False
    (audit / "label").write_text("seeded-shuffle")
    query = ray.data.read_parquet(
        str(root),
        shuffle=FileShuffleConfig(
            seed=7, reseed_after_execution=False, small_chunks_shuffle=True
        ),
    ).filter(expr=residual)
    plan = LogicalOptimizer().optimize(query._logical_plan)
    assert any(
        isinstance(node, Filter) and node.predicate_expr is not None
        for node in plan.dag.post_order_iter()
    )
    sources = [
        node for node in plan.dag.post_order_iter() if isinstance(node, ListFiles)
    ]
    assert sources
    assert all(
        getattr(node.file_indexer, "_listing_hint", None) is None for node in sources
    )
    result = query.materialize()
    assert not state["hint_seen"]
    assert sorted(row["x"] for row in result.take_all()) == list(range(40)) + list(
        range(70, 80)
    )
    schema = result.schema(fetch_if_missing=False)
    assert schema is not None
    assert schema.base_schema.equals(pa.schema([("x", pa.int64()), ("p", pa.string())]))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
