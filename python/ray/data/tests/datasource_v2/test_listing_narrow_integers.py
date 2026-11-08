"""Observe signed-width pruning through real worker read completions."""

import json
import os
from collections import Counter
from dataclasses import replace

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


def test_narrow_integer_consumed_worker_groups(tmp_path, monkeypatch):
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
        for dtype in [pa.int8(), pa.int16(), pa.int32()]:
            for position in ["partition", "filter", "payload"]:
                root = tmp_path / f"{dtype}-{position}"
                paths = []
                expected_rows = []
                p_type = dtype if position == "partition" else pa.int64()
                x_type = dtype if position == "filter" else pa.int64()
                for partition in [0, 1]:
                    path = root / f"p={partition}" / "data.parquet"
                    path.parent.mkdir(parents=True)
                    table = pa.table({"x": pa.array(range(40), type=x_type)})
                    if position == "payload":
                        table = table.append_column(
                            "payload", pa.array([None, -1, 0, 1] * 10, type=dtype)
                        )
                    pq.write_table(table, path, row_group_size=10)
                    paths.append(str(path))
                    expected_rows += [
                        dict(row, p=partition) for row in table.slice(30).to_pylist()
                    ]
                schema = table.schema.append(pa.field("p", p_type))
                residual = (col("p") == 99) | (col("x") >= 30)
                for with_q in [False, True]:
                    for enabled in [False, True]:
                        mode["enabled"] = enabled
                        label = f"{dtype}-{position}-q{with_q}-hint{enabled}"
                        (audit / "label").write_text(label)
                        data = ray.data.read_parquet(
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
                        if with_q:
                            data = data.filter(expr=col("x") >= 10)
                        materialized = data.filter(expr=residual).materialize()
                        rows = sorted(
                            materialized.take_all(),
                            key=lambda row: (row["p"], row["x"]),
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
                            return Counter(
                                (r["path"], i) for r in events for i in r["groups"]
                            )

                        assert starts and len(starts) == len(done)
                        assert all(r["pid"] != os.getpid() for r in done)
                        assert units(starts) == units(done)
                        expected_ids = (
                            [3] if enabled else ([1, 2, 3] if with_q else [0, 1, 2, 3])
                        )
                        assert units(done) == Counter(
                            (path, i) for path in paths for i in expected_ids
                        )
                        if enabled:
                            assert sum(units(done).values()) < (6 if with_q else 8)
                        if with_q:
                            assert all(r["q"] != "None" for r in done)
                        summaries.append(
                            {
                                "label": label,
                                "groups_per_file": expected_ids,
                                "rows": len(rows),
                                "worker_pids": sorted({r["pid"] for r in done}),
                                "schema": str(schema),
                            }
                        )
        if os.getenv("NARROW_INTEGER_EVIDENCE_DIR"):
            from pathlib import Path

            out = Path(os.environ["NARROW_INTEGER_EVIDENCE_DIR"])
            (out / "worker-cases.json").write_text(
                json.dumps(summaries, indent=2) + "\n"
            )
            for path in audit.glob("*.jsonl"):
                (out / path.name).write_bytes(path.read_bytes())
    finally:
        ray.shutdown()
        DataContext._set_current(previous)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
