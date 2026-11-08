"""Real worker boundary partitions, R-before-Q and string partition coverage."""

import json
import os
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
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


def test_boundary_partitions_and_residual_before_q(tmp_path, monkeypatch):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    audit = tmp_path / "audit"
    audit.mkdir()
    audit_path = str(audit)

    def setup_worker():
        from ray.data.tests.datasource_v2.test_listing_pruning import (
            _install_read_audit,
        )

        _install_read_audit(audit_path)

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
    previous = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.enable_progress_bars = False
    context.enable_operator_progress_bars = False
    context.execution_options.preserve_order = False
    context.max_errored_blocks = 0
    DataContext._set_current(context)
    summaries = []
    try:
        ray.init(
            num_cpus=2,
            include_dashboard=False,
            runtime_env={"worker_process_setup_hook": setup_worker},
        )
        cases = [
            (dtype, False) for dtype in [pa.int8(), pa.int16(), pa.int32(), pa.int64()]
        ] + [(pa.int16(), True)]
        for dtype, string_partition in cases:
            low = -(2 ** (dtype.bit_width - 1))
            high = 2 ** (dtype.bit_width - 1) - 1
            values = [
                low,
                low + 1,
                low + 2,
                -3,
                -2,
                -1,
                -1,
                0,
                1,
                high - 2,
                high - 1,
                high,
            ]
            partitions = ["a", "b"] if string_partition else [low, high]
            with_q = not string_partition
            root = tmp_path / f"{dtype}-string{string_partition}"
            expected_rows = []
            paths = []
            table = pa.table({"x": pa.array(values, type=dtype), "rid": range(12)})
            for partition in partitions:
                path = root / f"p={partition}" / "data.parquet"
                path.parent.mkdir(parents=True)
                paths.append(str(path))
                pq.write_table(table, path, row_group_size=3)
                for row in table.to_pylist():
                    if (partition == partitions[0] or row["x"] >= high - 1) and (
                        not with_q or row["x"] >= -3
                    ):
                        expected_rows.append(dict(row, p=partition))
            p_type = pa.string() if string_partition else dtype
            schema = table.schema.append(pa.field("p", p_type))
            residual = (col("p") == partitions[0]) | (col("x") >= high - 1)
            q = col("x") >= -3
            for enabled in [False, True]:
                mode["enabled"] = enabled
                label = f"{dtype}-string{string_partition}-hint{enabled}"
                (audit / "label").write_text(label)
                data = ray.data.read_parquet(
                    str(root),
                    schema=schema,
                    partitioning=Partitioning(
                        PartitionStyle.HIVE,
                        base_dir=str(root),
                        field_names=["p"],
                        field_types={"p": str if string_partition else int},
                    ),
                    override_num_blocks=1,
                )
                data = data.filter(expr=residual)
                if with_q:
                    # Explicitly exercise the previously missing R -> Q order.
                    data = data.filter(expr=q)
                materialized = data.materialize()
                rows = sorted(
                    materialized.take_all(), key=lambda row: (row["p"], row["rid"])
                )
                assert rows == expected_rows
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
                baseline_ids = [1, 2, 3] if with_q else [0, 1, 2, 3]
                expected_ids = [
                    baseline_ids,
                    [3] if enabled else baseline_ids,
                ]
                assert units(done) == Counter(
                    (path, i) for path, ids in zip(paths, expected_ids) for i in ids
                )
                if enabled:
                    assert sum(units(done).values()) < 2 * len(baseline_ids)
                if with_q:
                    assert all(r["q"] != "None" for r in done)
                summaries.append(
                    {
                        "label": label,
                        "groups_by_path": dict(zip(paths, expected_ids)),
                        "rows": rows,
                        "worker_pids": sorted({r["pid"] for r in done}),
                        "schema": str(schema),
                    }
                )
        if os.getenv("NARROW_INTEGER_EVIDENCE_DIR"):
            out = Path(os.environ["NARROW_INTEGER_EVIDENCE_DIR"])
            (out / "boundary-worker-cases.json").write_text(
                json.dumps(summaries, indent=2) + "\n"
            )
            for path in audit.glob("*.jsonl"):
                (out / ("boundary-" + path.name)).write_bytes(path.read_bytes())
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
