"""Permanent Worker regressions for nullable IN, version bounds and float bits."""
import json
import os
import struct
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


def _expected_ids(case, partition, enabled, version):
    """Pinned native outcomes, without witness groups; preserve duplicate multiplicities.

    Exact negative performance outcomes apply only to the measured releases.
    Future Arrow may legitimately improve statistics simplification.
    """
    repeated = case.startswith("q_repeat")
    baseline = [0, 1, 3, 4, 5] if repeated else list(range(6))
    if not enabled:
        return baseline
    if case in ("empty_set", "null_set"):
        return baseline
    if version not in ("17.0.0", "23.0.1", "25.0.1"):
        return None
    if case in ("nullable_not_in", "in_len_51", "in_len_256"):
        return baseline
    if case == "partition_in":
        return baseline if partition == 30 else [0, 1, 5]
    if case in ("string_in", "bool_in"):
        # Arrow 17 retains the nullable first group even without a witness.
        return [0, 3, 4, 5] if version == "17.0.0" else [3, 4, 5]
    if case == "partition_true":
        return (
            baseline
            if partition == 30
            else ([0, 1, 3, 4, 5] if version == "17.0.0" else [0, 1, 5])
        )
    if repeated:
        return baseline if version == "17.0.0" else [0, 1, 5]
    assert case == "in_len_50"
    return baseline if version == "17.0.0" else [5]


def _fixture(root):
    patterns = [
        0x0000000000000000,
        0x8000000000000000,
        0x7FF8000000000001,
        0xFFF8000000000042,
        0x3FF0000000000000,
        0x7FF0000000000000,
    ]
    expected = []
    for partition in [30, 40]:
        path = root / f"p={partition}" / "data.parquet"
        path.parent.mkdir(parents=True)
        x = [None if i in [5, 15, 55] or 20 <= i < 30 else i for i in range(60)]
        strings = [
            None if i in [5, 45] else "abcde"[min(i // 10, 4)] for i in range(60)
        ]
        booleans = [None if i in [5, 35] else i >= 30 for i in range(60)]
        raw = b"".join(struct.pack("<Q", patterns[i % 6]) for i in range(60))
        floats = pa.Array.from_buffers(pa.float64(), 60, [None, pa.py_buffer(raw)])
        table = pa.table(
            {
                "rid": list(range(60)),
                "x": pa.array(x, type=pa.int64()),
                "s": strings,
                "b": booleans,
                "f": floats,
            }
        )
        pq.write_table(table, path, row_group_size=10)
        assert (
            pq.ParquetFile(path)
            .read()
            .column("f")
            .combine_chunks()
            .buffers()[1]
            .to_pybytes()
            == raw
        )
        for i in range(60):
            expected.append(
                {
                    "p": partition,
                    "rid": i,
                    "x": x[i],
                    "s": strings[i],
                    "b": booleans[i],
                    "f_bits": raw[i * 8 : (i + 1) * 8].hex(),
                }
            )
    return table.schema.append(pa.field("p", pa.int64())), expected


def _predicate(case):
    if case.startswith("in_len_"):
        members = list(range(50, 50 + int(case.rsplit("_", 1)[1])))
        return (col("p") == 99) | col("rid").is_in(members), lambda row: row[
            "rid"
        ] in members
    if case == "nullable_not_in":
        members = list(range(10, 20)) + list(range(30, 40))
        return (
            ~((col("p") == 99) | col("x").is_in(members)),
            lambda row: row["x"] not in members,
        )
    if case == "empty_set":
        return (col("p") == 99) | col("x").is_in([]), lambda row: False
    if case == "null_set":
        return (col("p") == 99) | col("x").is_in([50, None]), lambda row: row["x"] in [
            50,
            None,
        ]
    if case == "partition_in":
        return col("p").is_in([30]) | (col("x") >= 50), lambda row: row["p"] == 30 or (
            row["x"] is not None and row["x"] >= 50
        )
    if case == "string_in":
        return (col("p") == 99) | col("s").is_in(["d", "e"]), lambda row: row["s"] in [
            "d",
            "e",
        ]
    if case == "bool_in":
        return (col("p") == 99) | col("b").is_in([True]), lambda row: row["b"] is True
    if case == "partition_true":
        return (col("p") == 30) | col("x").is_in([50, 51]), lambda row: row[
            "p"
        ] == 30 or row["x"] in [50, 51]
    return (col("p") == 99) | col("x").is_in([50, 51]), lambda row: row["x"] in [50, 51]


def test_listing_hint_worker_edges_and_float_bits(tmp_path, monkeypatch):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    root = tmp_path / "data"
    schema, original = _fixture(root)
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
    old = DataContext.get_current()
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
        for case in [
            "nullable_not_in",
            "empty_set",
            "null_set",
            "partition_in",
            "string_in",
            "bool_in",
            "partition_true",
            "q_repeat_before",
            "q_repeat_after",
            "in_len_50",
            "in_len_51",
            "in_len_256",
        ]:
            residual, predicate = _predicate(case)
            repeated = case.startswith("q_repeat")
            partitions = [30, 30, 40] if repeated else [30, 40]
            paths = [str(root / f"p={p}" / "data.parquet") for p in partitions]
            expected = (
                original + [r for r in original if r["p"] == 30]
                if repeated
                else original
            )
            expected = sorted(
                [
                    r
                    for r in expected
                    if predicate(r)
                    and (not repeated or (isinstance(r["x"], int) and r["x"] >= 10))
                ],
                key=lambda r: (r["p"], r["rid"]),
            )
            baseline: Counter[tuple[str, int]] | None = None
            for enabled in [False, True]:
                mode["enabled"] = enabled
                label = f"{case}-{enabled}"
                (audit / "label").write_text(label)
                data = ray.data.read_parquet(
                    paths,
                    schema=schema,
                    partitioning=Partitioning(
                        PartitionStyle.HIVE,
                        base_dir=str(root),
                        field_names=["p"],
                        field_types={"p": int},
                    ),
                    override_num_blocks=1,
                )
                if case == "q_repeat_before":
                    data = data.filter(expr=col("x") >= 10)
                data = data.filter(expr=residual)
                if case == "q_repeat_after":
                    data = data.filter(expr=col("x") >= 10)
                result = data.materialize()
                assert result.schema().base_schema.equals(schema, check_metadata=True)
                rows = []
                for batch in result.iter_batches(batch_format="pyarrow"):
                    assert batch.schema.equals(schema, check_metadata=True)
                    floats = batch.column("f").combine_chunks()
                    for i, row in enumerate(batch.to_pylist()):
                        row.pop("f")
                        row["f_bits"] = (
                            floats.buffers()[1]
                            .slice((floats.offset + i) * 8, 8)
                            .to_pybytes()
                            .hex()
                        )
                        rows.append(row)
                assert sorted(rows, key=lambda r: (r["p"], r["rid"])) == expected
                records = [
                    json.loads(line)
                    for p in audit.glob("*.jsonl")
                    for line in p.read_text().splitlines()
                ]
                records = [r for r in records if r["label"] == label]
                starts = [r for r in records if r["event"] == "start"]
                done = [r for r in records if r["event"] == "complete"]

                def units(events):
                    return Counter((r["path"], i) for r in events for i in r["groups"])

                actual = units(done)
                assert starts and len(starts) == len(done) and units(starts) == actual
                assert all(r["pid"] != os.getpid() for r in done)
                if repeated:
                    assert all(r["q"] != "None" for r in starts)
                ids = [
                    _expected_ids(case, p, enabled, pa.__version__) for p in partitions
                ]
                if all(group is not None for group in ids):
                    assert actual == Counter(
                        (path, i)
                        for path, group in zip(paths, ids)
                        if group is not None
                        for i in group
                    ), (case, enabled, actual)
                if not enabled:
                    baseline = actual
                else:
                    assert baseline is not None
                    assert actual <= baseline
                    if case in ("empty_set", "null_set"):
                        assert actual == baseline
                    positive = case in (
                        "partition_in",
                        "string_in",
                        "bool_in",
                        "partition_true",
                    ) or (
                        pa.__version__ in ("23.0.1", "25.0.1")
                        and (repeated or case == "in_len_50")
                    )
                    if positive:
                        assert sum(actual.values()) < sum(baseline.values()), case
    finally:
        ray.shutdown()
        DataContext._set_current(old)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
