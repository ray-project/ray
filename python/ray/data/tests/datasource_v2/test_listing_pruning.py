"""Public API tests that observe consumed row groups inside real read workers."""
import json
import math
import os
import weakref
from collections import Counter
from dataclasses import replace
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

import ray
import ray.data
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _footer_indexer(value):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    assert isinstance(value, FooterFileIndexer)
    return value


def _install_read_audit(directory):
    # Test-only observer: preserve native arguments, returned objects and tables.
    from ray.data._internal.datasource_v2.common.file_reader import FileReader
    from ray.data._internal.datasource_v2.formats.parquet import parquet_file_reader

    tracked = {}
    native_fragments = parquet_file_reader._fragments_from_row_group_ids
    native_read = FileReader._iter_unit_tables

    def fragments(fragment, row_group_ids, **kwargs):
        ids = [int(i) for i in row_group_ids]
        result = native_fragments(fragment, row_group_ids, **kwargs)
        assert not kwargs["per_row_group_offsets"]
        for item in result:
            key = id(item)

            def discard(reference, key=key):
                if tracked.get(key, (None,))[0] is reference:
                    tracked.pop(key)

            tracked[key] = (weakref.ref(item, discard), ids)
        return result

    def emit(event, item, ids, predicate):
        label = (Path(directory) / "label").read_text()
        record = {
            "label": label,
            "event": event,
            "q": str(predicate),
            "pid": os.getpid(),
            "path": item.unit.source,
            "groups": ids,
        }
        with (Path(directory) / f"{os.getpid()}.jsonl").open("a") as handle:
            handle.write(json.dumps(record) + "\n")

    def read(self, fragment_to_read, scanner_kwargs):
        reference, ids = tracked[id(fragment_to_read)]
        item = reference()
        assert item is fragment_to_read
        emit("start", item, ids, self._predicate)
        yield from native_read(self, fragment_to_read, scanner_kwargs)
        emit("complete", item, ids, self._predicate)
        del tracked[id(item)]

    parquet_file_reader._fragments_from_row_group_ids = fragments
    FileReader._iter_unit_tables = read


def test_listing_hint_reduces_actual_worker_reads(tmp_path, monkeypatch):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    audit = tmp_path / "audit"
    audit.mkdir()
    audit_path = str(audit)

    def setup_worker():
        # Resolve the worker module at execution time, as the serialization
        # contract requires; do not capture the driver's patched function.
        import importlib

        module = importlib.import_module(
            "ray.data.tests.datasource_v2.test_listing_pruning"
        )
        module._install_read_audit(audit_path)

    mode = {"enabled": False}
    native_apply = DeriveListFilesPushdown.apply

    def apply(self, plan):
        result = native_apply(self, plan)
        if not mode["enabled"]:

            def clear(node):
                if isinstance(node, ListFiles):
                    return replace(
                        node,
                        file_indexer=_footer_indexer(
                            node.file_indexer
                        ).with_listing_hint(None),
                    )
                return node

            result._dag = result.dag._apply_transform(clear)
        return result

    monkeypatch.setattr(DeriveListFilesPushdown, "apply", apply)
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    previous_context = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.enable_progress_bars = False
    context.enable_operator_progress_bars = False
    context.execution_options.preserve_order = False
    context.max_errored_blocks = 0
    DataContext._set_current(context)
    try:
        ray.init(
            num_cpus=2,
            include_dashboard=False,
            runtime_env={"worker_process_setup_hook": setup_worker},
        )
        for case in ["in", "column", "float_payload", "no_statistics"]:
            root = tmp_path / case
            expected_groups = []
            for partition in [30, 40]:
                path = root / f"p={partition}" / "data.parquet"
                path.parent.mkdir(parents=True)
                table = pa.table({"x": list(range(60))})
                if case == "float_payload":
                    table = table.append_column(
                        "f",
                        pa.array(
                            [None, float("nan"), float("inf"), -0.0] * 15,
                            type=pa.float64(),
                        ),
                    )
                pq.write_table(
                    table,
                    path,
                    row_group_size=10,
                    write_statistics=case != "no_statistics",
                )
                # Independent native Arrow oracle with known fixture values.
                expr = (
                    ds.field("x") >= ds.scalar(partition)
                    if case == "column"
                    else (ds.scalar(partition) == 99) | (ds.field("x") >= 50)
                    if case == "float_payload"
                    else (ds.scalar(partition) == 99) | ds.field("x").isin([50, 51])
                )
                fragment = ds.ParquetFileFormat().make_fragment(
                    str(path), filesystem=fs.LocalFileSystem()
                )
                ids = {
                    part.row_groups[0].id
                    for part in fragment.split_by_row_group(expr, schema=table.schema)
                }
                expected_groups.extend((str(path), i) for i in ids)
            schema = table.schema.append(pa.field("p", pa.int64()))
            residual = (
                col("x") >= col("p")
                if case == "column"
                else (col("p") == 99) | (col("x") >= 50)
                if case == "float_payload"
                else (col("p") == 99) | col("x").is_in([50, 51])
            )
            expected_rows = []
            for partition in [30, 40]:
                for x in (
                    range(partition, 60)
                    if case == "column"
                    else range(50, 60)
                    if case == "float_payload"
                    else [50, 51]
                ):
                    row: dict[str, int | float | None] = {"p": partition, "x": x}
                    if case == "float_payload":
                        row["f"] = [None, float("nan"), float("inf"), -0.0][x % 4]
                    expected_rows.append(row)

            def normalize(rows):
                return [
                    {
                        k: "NaN" if isinstance(v, float) and math.isnan(v) else v
                        for k, v in row.items()
                    }
                    for row in rows
                ]

            for enabled in [False, True]:
                mode["enabled"] = enabled
                label = f"{case}-{enabled}"
                (audit / "label").write_text(label)
                materialized = (
                    ray.data.read_parquet(
                        str(root),
                        schema=schema,
                        partitioning=Partitioning(
                            PartitionStyle.HIVE,
                            base_dir=str(root),
                            field_names=["p"],
                            field_types={"p": int},
                        ),
                        override_num_blocks=1,
                    )
                    .filter(expr=residual)
                    .materialize()
                )
                rows = sorted(
                    materialized.take_all(), key=lambda row: (row["p"], row["x"])
                )
                assert normalize(rows) == normalize(expected_rows)
                assert materialized.schema().base_schema.equals(
                    schema, check_metadata=True
                )
                records = [
                    json.loads(line)
                    for path in audit.glob("*.jsonl")
                    for line in path.read_text().splitlines()
                ]
                records = [r for r in records if r["label"] == label]
                starts = [r for r in records if r["event"] == "start"]
                done = [r for r in records if r["event"] == "complete"]

                def units(events):
                    return Counter((r["path"], i) for r in events for i in r["groups"])

                assert starts and len(starts) == len(done)
                assert all(r["pid"] != os.getpid() for r in done)
                assert units(starts) == units(done)
                if enabled:
                    assert units(done) == Counter(expected_groups)
                    if case in ("column", "float_payload"):
                        assert sum(units(done).values()) < 12
                    if case == "in" and pa.__version__ == "17.0.0":
                        assert sum(units(done).values()) == 12
                else:
                    assert sum(units(done).values()) == 12
                if case == "no_statistics":
                    assert sum(units(done).values()) == 12
    finally:
        ray.shutdown()
        DataContext._set_current(previous_context)


def test_single_group_empty_residual_preserves_schema(tmp_path):
    directory = tmp_path / "p=b"
    directory.mkdir()
    pq.write_table(pa.table({"x": [1, 2, 3]}), directory / "data.parquet")
    schema = pa.schema([("x", pa.int64()), ("p", pa.string())])
    ray.init(
        num_cpus=1,
        include_dashboard=False,
        object_store_memory=128 * 1024 * 1024,
        log_to_driver=False,
    )
    context = DataContext.get_current()
    old_v2 = context.use_datasource_v2
    old_order = context.execution_options.preserve_order
    try:
        context.use_datasource_v2 = True
        context.execution_options.preserve_order = False
        result = (
            ray.data.read_parquet(
                str(tmp_path),
                schema=schema,
                partitioning=Partitioning(
                    PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
                ),
                override_num_blocks=1,
            )
            .filter(expr=(col("p") == "missing") | (col("x") > 100))
            .materialize()
        )
        assert result.count() == 0
        assert result.schema().base_schema.equals(schema, check_metadata=True)
    finally:
        context.use_datasource_v2 = old_v2
        context.execution_options.preserve_order = old_order
        ray.shutdown()


@pytest.mark.parametrize(
    "case",
    ["plain", "duplicate", "multi_schema", "projection_empty", "binary", "metadata"],
)
def test_rowmap_keeps_full_worker_reads(tmp_path, monkeypatch, case):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    audit = tmp_path / "audit"
    audit.mkdir()
    audit_path = str(audit)

    def setup_worker():
        import importlib

        importlib.import_module(
            "ray.data.tests.datasource_v2.test_listing_pruning"
        )._install_read_audit(audit_path)

    root = tmp_path / "data"
    directory = root / "p=b"
    directory.mkdir(parents=True)
    physical = pa.table(
        {"x": range(40), "payload": [b"z"] * 40 if case == "binary" else ["z"] * 40}
    )
    if case == "metadata":
        physical = physical.replace_schema_metadata(
            {b"description": b"normal metadata"}
        )
    path = directory / "data.parquet"
    pq.write_table(physical, path, row_group_size=10)
    paths = [str(path)]
    if case == "duplicate":
        paths *= 2
    if case == "multi_schema":
        second = directory / "second.parquet"
        pq.write_table(
            physical.append_column("unused", pa.array(range(40))),
            second,
            row_group_size=10,
        )
        paths.append(str(second))
    schema = physical.schema.append(pa.field("p", pa.string()))
    previous = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.execution_options.preserve_order = False
    DataContext._set_current(context)
    enabled = False
    native_apply = DeriveListFilesPushdown.apply
    modes = []

    def apply(self, plan):
        configured = native_apply(self, plan)
        if enabled:
            modes.append(
                any(
                    isinstance(node, ListFiles)
                    and _footer_indexer(node.file_indexer)._listing_hint is not None
                    for node in configured.dag.post_order_iter()
                )
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
        num_cpus=1,
        include_dashboard=False,
        object_store_memory=128 * 1024 * 1024,
        log_to_driver=False,
        runtime_env={"worker_process_setup_hook": setup_worker},
    )
    schemas = []
    rows_by_lane = []
    try:
        for enabled in [False, True]:
            label = str(enabled)
            (audit / "label").write_text(label)
            result = ray.data.read_parquet(
                paths,
                schema=schema,
                partitioning=Partitioning(
                    PartitionStyle.HIVE, base_dir=str(root), field_names=["p"]
                ),
                override_num_blocks=1,
            )
            result = result.filter(
                expr=(col("p") == "a")
                | (col("x") >= (100 if case == "projection_empty" else 30))
            )
            if case == "projection_empty":
                result = result.select_columns(["x", "p"])
            result = result.map(lambda row: dict(row, x=row["x"] + 1)).materialize()
            rows = sorted(result.take_all(), key=lambda row: row["x"])
            rows_by_lane.append(rows)
            assert len(rows) == (0 if case == "projection_empty" else 10 * len(paths))
            schema_result = result.schema()
            schemas.append(None if schema_result is None else schema_result.base_schema)
            records = [
                json.loads(line)
                for f in audit.glob("*.jsonl")
                for line in f.read_text().splitlines()
            ]
            records = [record for record in records if record["label"] == label]
            # MapRows is outside the root Filter shape: both lanes read all groups.
            selected = [0, 1, 2, 3]
            expected = Counter((path, group) for path in paths for group in selected)
            for event in ["start", "complete"]:
                actual = Counter(
                    (record["path"], group)
                    for record in records
                    if record["event"] == event
                    for group in record["groups"]
                )
                assert actual == expected
            assert records and all(record["pid"] != os.getpid() for record in records)
        assert modes and not any(modes)
        assert rows_by_lane[0] == rows_by_lane[1]
        assert (schemas[0] is None) == (schemas[1] is None)
        if schemas[0] is not None:
            assert schemas[1] is not None
            assert schemas[0].equals(schemas[1], check_metadata=True)
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


@pytest.mark.parametrize("q_first", [False, True])
@pytest.mark.parametrize("empty", [False, True])
def test_rowmap_q_and_empty_against_disabled_hint(
    tmp_path, monkeypatch, empty, q_first
):
    audit = tmp_path / "audit"
    audit.mkdir()
    (audit / "label").write_text("rowmap-q")
    audit_path = str(audit)

    def setup_worker():
        import importlib

        importlib.import_module(
            "ray.data.tests.datasource_v2.test_listing_pruning"
        )._install_read_audit(audit_path)

    root = tmp_path / "data"
    directory = root / "p=b"
    directory.mkdir(parents=True)
    path = directory / "data.parquet"
    pq.write_table(
        pa.table({"x": range(40), "q": pa.array([None, 1] * 20, type=pa.int64())}),
        path,
        row_group_size=10,
    )
    schema = pa.schema([("x", pa.int64()), ("q", pa.int64()), ("p", pa.string())])
    previous = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.execution_options.preserve_order = False
    DataContext._set_current(context)
    ray.init(
        num_cpus=1,
        include_dashboard=False,
        object_store_memory=128 * 1024 * 1024,
        log_to_driver=False,
        runtime_env={"worker_process_setup_hook": setup_worker},
    )
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    enabled = False
    native_apply = DeriveListFilesPushdown.apply

    def apply(self, plan):
        configured = native_apply(self, plan)
        if enabled:
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

    def shift(row, delta=0, *, scale=1):
        return dict(row, x=(row["x"] + delta) * scale)

    observed_schemas = []
    try:
        for enabled in [False, True]:
            label = f"q-empty-{empty}-hint-{enabled}"
            (audit / "label").write_text(label)
            source = ray.data.read_parquet(
                str(root),
                schema=schema,
                partitioning=Partitioning(
                    PartitionStyle.HIVE, base_dir=str(root), field_names=["p"]
                ),
                override_num_blocks=1,
            )
            predicates = [
                col("x") >= 10,
                (col("p") == "a") | (col("x") >= (100 if empty else 30)),
            ]
            for predicate in predicates if q_first else reversed(predicates):
                source = source.filter(expr=predicate)
            result = source.map(
                shift, fn_args=(1,), fn_kwargs={"scale": 2}
            ).materialize()
            rows = sorted(result.take_all(), key=lambda row: row["x"])
            assert rows == (
                []
                if empty
                else [
                    {"x": (i + 1) * 2, "q": None if i % 2 == 0 else 1, "p": "b"}
                    for i in range(30, 40)
                ]
            )
            result_schema = result.schema()
            observed_schemas.append(
                None if result_schema is None else result_schema.base_schema
            )
            if not empty:
                assert result_schema is not None
                assert result_schema.base_schema.equals(schema, check_metadata=True)
            records = [
                json.loads(line)
                for f in audit.glob("*.jsonl")
                for line in f.read_text().splitlines()
            ]
            records = [r for r in records if r["label"] == label]
            starts = Counter(
                (r["path"], g)
                for r in records
                if r["event"] == "start"
                for g in r["groups"]
            )
            done = Counter(
                (r["path"], g)
                for r in records
                if r["event"] == "complete"
                for g in r["groups"]
            )
            selected = [1, 2, 3]
            assert starts == done == Counter((str(path), g) for g in selected)
            assert records and all(r["pid"] != os.getpid() for r in records)
            assert all(r["q"] != "None" and "10" in r["q"] for r in records)
        before, after = observed_schemas
        assert (before is None) == (after is None)
        if before is not None:
            assert after is not None
            assert before.equals(after, check_metadata=True)
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


@pytest.mark.parametrize(
    "mode", ["async", "actor", "limit", "map_batches", "preserve_order"]
)
def test_public_map_plan_declines_unsupported_ancestors(tmp_path, mode):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    directory = tmp_path / "p=b"
    directory.mkdir()
    pq.write_table(
        pa.table({"x": range(40)}), directory / "data.parquet", row_group_size=10
    )
    schema = pa.schema([("x", pa.int64()), ("p", pa.string())])
    previous = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.execution_options.preserve_order = mode == "preserve_order"
    DataContext._set_current(context)
    ray.init(
        num_cpus=1,
        include_dashboard=False,
        log_to_driver=False,
        object_store_memory=128 * 1024 * 1024,
    )
    try:
        query = ray.data.read_parquet(
            str(tmp_path),
            schema=schema,
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
            ),
        ).filter(expr=(col("p") == "a") | (col("x") >= 30))

        async def asynchronous(row):
            return row

        class Identity:
            def __call__(self, row):
                return row

        # Exercise runtime public API inputs: Dataset.map's callable annotation
        # is narrower than its documented callable-class support. The async
        # input is deliberately plan-only; this test does not execute that UDF.
        options: dict[str, Any]
        if mode == "async":
            options = {"fn": asynchronous}
            query = query.map(**options)
        elif mode == "actor":
            options = {"fn": Identity, "compute": ray.data.ActorPoolStrategy(size=1)}
            query = query.map(**options)
        elif mode == "map_batches":
            query = query.map_batches(lambda batch: batch)
        elif mode == "preserve_order":
            query = query.map(lambda row: row)
        else:
            query = query.map(lambda row: row).limit(1)
        optimized = LogicalOptimizer().optimize(query._logical_plan)
        sources = [
            node
            for node in optimized.dag.post_order_iter()
            if isinstance(node, ListFiles)
        ]
        assert sources
        assert all(
            _footer_indexer(node.file_indexer)._listing_hint is None for node in sources
        )
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
