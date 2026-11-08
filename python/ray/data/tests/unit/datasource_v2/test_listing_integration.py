"""Positive listing/optimizer component tests; no bad cast/partition reader tests."""
from dataclasses import replace

import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import FooterReader
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import FileInfo
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators import Filter, ListFiles, ReadFiles
from ray.data._internal.logical.operators.count_operator import Count
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.logical.rules.derive_list_files_pushdown import (
    DeriveListFilesPushdown,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _footer_indexer(value):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    assert isinstance(value, FooterFileIndexer)
    return value


def plan(tmp_path, q=None, r=None):
    schema = pa.schema([("x", pa.int64()), ("p", pa.string())])
    scanner = ParquetScanner(
        schema=schema,
        predicate=q,
        partitioning=Partitioning(
            PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
        ),
    )
    indexer = FooterFileIndexer(ignore_missing_paths=False)
    listing = ListFiles(
        paths=[str(tmp_path)],
        source_paths=[str(tmp_path)],
        filesystem=fs.LocalFileSystem(),
        file_indexer=indexer,
    )
    read = ReadFiles(
        datasource_name="Parquet",
        scanner=scanner,
        schema=schema,
        parallelism=1,
        input_dependencies=[listing],
    )
    residual = r if r is not None else (col("p") == "a") | (col("x") >= 30)
    root = Filter(predicate_expr=residual, input_dependencies=[read])
    context = DataContext()
    context.execution_options.preserve_order = False
    return LogicalPlan(root, context)


def listing_of(plan):
    return next(n for n in plan.dag.post_order_iter() if isinstance(n, ListFiles))


def file(tmp_path, partition="b"):
    directory = tmp_path / f"p={partition}"
    directory.mkdir(exist_ok=True)
    path = directory / "part.parquet"
    pq.write_table(pa.table({"x": list(range(40))}), path, row_group_size=10)
    return FileInfo(str(path), path.stat().st_size)


def groups(info, q, hint):
    reader = FooterReader(
        fs.LocalFileSystem(), io_concurrency=1, filter_expr=q, listing_hint=hint
    )
    try:
        chunked = reader._read_and_chunk(info)
        return [i for group in chunked.row_groups for i in group.unit_ids], chunked
    finally:
        reader.pool.shutdown(wait=True)


@pytest.mark.parametrize("q", [None, col("x") >= 10])
def test_real_footer_selection_after_final_rule(tmp_path, q):
    original = plan(tmp_path, q)
    configured = DeriveListFilesPushdown().apply(original)
    hint = _footer_indexer(listing_of(configured).file_indexer)._listing_hint
    assert hint is not None
    assert _footer_indexer(listing_of(original).file_indexer)._listing_hint is None
    assert isinstance(configured.dag, Filter)
    assert isinstance(original.dag, Filter)
    assert configured.dag.predicate_expr is original.dag.predicate_expr
    configured_read = configured.dag.input_dependencies[0]
    assert isinstance(configured_read, ReadFiles)
    assert isinstance(configured_read.scanner, ParquetScanner)
    assert configured_read.scanner.predicate is q
    info = file(tmp_path)
    before, chunks = groups(info, q, None)
    after, reduced = groups(info, q, hint)
    assert before == ([0, 1, 2, 3] if q is None else [1, 2, 3])
    assert after == [3]
    assert reduced.schema_only_layout is not None
    assert len(after) < len(before)
    # H never turns an approximate Q count into an exact survivor count.
    old_flags = {i: g.fully_matched for g in chunks.row_groups for i in g.unit_ids}
    assert all(
        g.fully_matched == old_flags[i] for g in reduced.row_groups for i in g.unit_ids
    )
    assert not ray.is_initialized()


def test_partition_true_and_all_false_preserve_layout(tmp_path):
    configured = DeriveListFilesPushdown().apply(plan(tmp_path))
    hint = _footer_indexer(listing_of(configured).file_indexer)._listing_hint
    assert groups(file(tmp_path, "a"), None, hint)[0] == [0, 1, 2, 3]
    false_plan = plan(tmp_path, r=(col("p") == "a") | (col("x") > 100))
    hint = _footer_indexer(
        listing_of(DeriveListFilesPushdown().apply(false_plan)).file_indexer
    )._listing_hint
    ids, empty = groups(file(tmp_path, "b"), None, hint)
    assert ids == []
    assert empty.schema_only_layout is not None


def test_partial_q_prunes_but_optional_split_failure_keeps_baseline(tmp_path):
    q = (col("x") % 2) == 0
    configured = DeriveListFilesPushdown().apply(plan(tmp_path, q))
    hint = _footer_indexer(listing_of(configured).file_indexer)._listing_hint
    info = file(tmp_path)
    assert groups(info, q, None)[0] == [0, 1, 2, 3]
    selected, certified = groups(info, q, hint)
    assert selected == [3]
    assert certified.schema_only_layout is not None
    assert not certified.row_groups[0].fully_matched

    class CannotSplit:
        def split_by_row_group(self, *args, **kwargs):
            raise ValueError("optional statistics unavailable")

    reader = FooterReader(fs.LocalFileSystem(), io_concurrency=1, listing_hint=hint)
    try:
        metadata = pq.read_metadata(info.path)
        baseline = [0, 1, 2, 3]
        assert reader._apply_listing_hint(
            CannotSplit(), metadata, baseline, info.path
        ) == (baseline, False)
    finally:
        reader.pool.shutdown(wait=True)


@pytest.mark.parametrize(
    "mode",
    [
        "ordered",
        "error_budget",
        "count",
        "limit",
        "known_udf",
        "subclass",
        "unsupported_r",
    ],
)
def test_gate_clears_old_hint_before_declining(tmp_path, mode):
    initial = DeriveListFilesPushdown().apply(plan(tmp_path))
    assert _footer_indexer(listing_of(initial).file_indexer)._listing_hint is not None
    root, context = initial.dag, initial.context
    if mode == "ordered":
        context.execution_options.preserve_order = True
    elif mode == "error_budget":
        context.max_errored_blocks = 1
    elif mode == "count":
        root = Count(input_dependencies=[root])
    elif mode == "limit":
        read = root.input_dependencies[0]
        assert isinstance(read, ReadFiles)
        assert isinstance(read.scanner, ParquetScanner)
        root = root._with_new_input_dependencies(
            [replace(read, scanner=replace(read.scanner, limit=1))]
        )
    elif mode in ("known_udf", "subclass"):
        read = root.input_dependencies[0]
        assert isinstance(read, ReadFiles)
        assert isinstance(read.scanner, ParquetScanner)
        if mode == "known_udf":
            read = replace(
                read, block_udf=lambda table: table, block_udf_modified_columns=set()
            )
        else:

            class CustomScanner(ParquetScanner):
                def listing_pruning_context(self):
                    raise AssertionError("must not probe unknown scanner")

            read = replace(read, scanner=CustomScanner(schema=read.schema))
        root = root._with_new_input_dependencies([read])
    else:
        root = Filter(
            predicate_expr=col("x").is_in([1, None]),
            input_dependencies=root.input_dependencies,
        )
    result = DeriveListFilesPushdown().apply(LogicalPlan(root, context))
    assert _footer_indexer(listing_of(result).file_indexer)._listing_hint is None
    assert _footer_indexer(listing_of(initial).file_indexer)._listing_hint is not None


def test_real_optimizer_retains_residual_and_count_cannot_use_hint(tmp_path):
    source = plan(tmp_path, col("x") >= 10)
    optimized = LogicalOptimizer().optimize(source)
    assert _footer_indexer(listing_of(optimized).file_indexer)._listing_hint is not None
    assert any(isinstance(n, Filter) for n in optimized.dag.post_order_iter())
    assert isinstance(optimized.dag, Filter)
    count = LogicalPlan(Count(input_dependencies=[optimized.dag]), source.context)
    counted = LogicalOptimizer().optimize(count)
    assert isinstance(counted.dag, Count)
    assert any(isinstance(n, Filter) for n in counted.dag.post_order_iter())
    assert _footer_indexer(listing_of(counted).file_indexer)._listing_hint is None


def test_all_null_group_is_not_pruned_by_nonnull_bounds(tmp_path):
    original = plan(tmp_path, r=(col("p") == "a") | col("x").is_null())
    configured = DeriveListFilesPushdown().apply(original)
    hint = _footer_indexer(listing_of(configured).file_indexer)._listing_hint
    directory = tmp_path / "p=b"
    directory.mkdir()
    path = directory / "nulls.parquet"
    pq.write_table(
        pa.table(
            {
                "x": pa.array(
                    list(range(10)) + [None] * 10 + list(range(30, 40)), type=pa.int64()
                )
            }
        ),
        path,
        row_group_size=10,
    )
    info = FileInfo(str(path), path.stat().st_size)
    assert groups(info, None, None)[0] == [0, 1, 2]
    assert groups(info, None, hint)[0] == [1]


def test_actual_metadata_count_rewrite_discards_old_hint(tmp_path):
    configured = DeriveListFilesPushdown().apply(plan(tmp_path))
    read = configured.dag.input_dependencies[0]
    assert isinstance(read, ReadFiles)
    listing = read.input_dependencies[0]
    assert isinstance(listing, ListFiles)
    assert _footer_indexer(listing.file_indexer)._listing_hint is not None
    # Change the root to a count with no residual. Run the actual rewrite;
    # never hand-build the resulting metadata-count MapBatches consumer.
    count = LogicalPlan(Count(input_dependencies=[read]), configured.context)
    result = LogicalOptimizer().optimize(count)
    from ray.data._internal.logical.operators.map_operator import MapBatches

    assert isinstance(result.dag, MapBatches)
    listing = listing_of(result)
    assert getattr(listing.file_indexer, "_listing_hint", None) is None
    assert listing.predicate is None
    assert listing.file_partitioner is None
    assert not ray.is_initialized()


def test_public_api_builtin_retry_wrapper_is_admitted_without_unwrapping(tmp_path):
    from ray.data._internal.util import (
        RetryingPyFileSystem,
        RetryingPyFileSystemHandler,
    )

    source = plan(tmp_path)
    read = source.dag.input_dependencies[0]
    wrapper = RetryingPyFileSystem.wrap(fs.LocalFileSystem(), retryable_errors=[])
    assert isinstance(read, ReadFiles)
    assert isinstance(read.scanner, ParquetScanner)
    scanner = replace(read.scanner, filesystem=wrapper)
    assert scanner.listing_pruning_context() is not None
    assert scanner.filesystem is wrapper

    class CustomHandler(RetryingPyFileSystemHandler):
        def unwrap(self):
            raise AssertionError("unknown handler must not be invoked")

    unknown = RetryingPyFileSystem(CustomHandler(fs.LocalFileSystem()))
    assert replace(scanner, filesystem=unknown).listing_pruning_context() is None


@pytest.mark.parametrize(
    "rows_per_group, q, expected_id, fully_matched",
    [
        (40, None, 0, True),
        (10, col("x") >= 30, 3, True),
        (10, col("x") >= 35, 3, False),
    ],
)
def test_single_baseline_group_certifies_layout_without_changing_q_count(
    tmp_path, rows_per_group, q, expected_id, fully_matched
):
    info = file(tmp_path)
    pq.write_table(
        pa.table({"x": list(range(40))}), info.path, row_group_size=rows_per_group
    )
    hint = _footer_indexer(
        listing_of(DeriveListFilesPushdown().apply(plan(tmp_path))).file_indexer
    )._listing_hint
    for _ in range(2):
        ids, chunks = groups(info, q, hint)
        assert ids == [expected_id]
        assert chunks.row_groups[0].fully_matched is fully_matched
        assert chunks.schema_only_layout is not None


def test_two_baseline_groups_still_allow_hint_pruning(tmp_path):
    info = file(tmp_path)
    pq.write_table(pa.table({"x": list(range(40))}), info.path, row_group_size=20)
    residual = (col("p") == "missing") | (col("x") > 100)
    hint = _footer_indexer(
        listing_of(
            DeriveListFilesPushdown().apply(plan(tmp_path, r=residual))
        ).file_indexer
    )._listing_hint
    assert groups(info, None, None)[0] == [0, 1]
    assert groups(info, None, hint)[0] == []


@pytest.mark.parametrize(
    "kind", ["row", "chain", "batch_none", "batch_two", "class", "limit"]
)
@pytest.mark.parametrize("with_q", [False, True])
def test_local_rowmap_pruning_boundary(tmp_path, kind, with_q):
    from ray.data._internal.logical.operators.map_operator import MapBatches, MapRows

    original = plan(tmp_path, q=(col("x") >= 10) if with_q else None)

    def fn(row):
        return row

    root = MapRows(fn=fn, input_dependencies=[original.dag])
    if kind == "chain":
        root = MapRows(fn=fn, input_dependencies=[root])
    elif kind.startswith("batch"):
        root = MapBatches(
            fn=fn,
            input_dependencies=[original.dag],
            batch_size=None if kind == "batch_none" else 2,
        )
    elif kind == "class":

        class Identity:
            def __call__(self, row):
                return row

        root = MapRows(fn=Identity, input_dependencies=[original.dag])
    elif kind == "limit":
        root = replace(root, per_block_limit=1)
    configured = DeriveListFilesPushdown().apply(LogicalPlan(root, original.context))
    indexer = _footer_indexer(listing_of(configured).file_indexer)
    assert indexer._listing_hint is None
    assert _footer_indexer(listing_of(original).file_indexer)._listing_hint is None
    filters = [
        node for node in configured.dag.post_order_iter() if isinstance(node, Filter)
    ]
    assert len(filters) == 1
    assert isinstance(original.dag, Filter)
    assert filters[0].predicate_expr is original.dag.predicate_expr
    twice = DeriveListFilesPushdown().apply(configured)
    assert _footer_indexer(listing_of(twice).file_indexer)._listing_hint is None
    if kind in ("row", "chain"):
        assert groups(file(tmp_path), (col("x") >= 10) if with_q else None, None)[
            0
        ] == ([1, 2, 3] if with_q else [0, 1, 2, 3])
        assert isinstance(configured.dag, MapRows)
        assert configured.dag.fn is fn
        assert configured.dag.compute is root.compute


@pytest.mark.parametrize("mode", ["async", "actor", "outer_limit", "ordered"])
def test_rowmap_stale_hint_cleared_at_new_boundary(tmp_path, mode):
    from ray.data._internal.compute import ActorPoolStrategy
    from ray.data._internal.logical.operators import Limit
    from ray.data._internal.logical.operators.map_operator import MapRows

    def identity(row):
        return row

    async def asynchronous(row):
        return row

    original = plan(tmp_path)
    configured = DeriveListFilesPushdown().apply(original)
    assert (
        _footer_indexer(listing_of(configured).file_indexer)._listing_hint is not None
    )
    root = MapRows(fn=identity, input_dependencies=[configured.dag])
    if mode == "async":
        root = replace(root, fn=asynchronous)
    elif mode == "actor":
        root = replace(root, compute=ActorPoolStrategy(size=1))
    elif mode == "ordered":
        original.context.execution_options.preserve_order = True
    else:
        root = Limit(limit=1, input_dependencies=[root])
    declined = DeriveListFilesPushdown().apply(LogicalPlan(root, original.context))
    assert _footer_indexer(listing_of(declined).file_indexer)._listing_hint is None
    assert (
        _footer_indexer(listing_of(configured).file_indexer)._listing_hint is not None
    )


def test_shared_source_rowmap_branches_are_independent(tmp_path):
    from ray.data._internal.logical.operators.map_operator import MapRows
    from ray.data._internal.logical.operators.n_ary_operator import Union

    original = plan(tmp_path)

    def identity(row):
        return row

    first = DeriveListFilesPushdown().apply(original)
    mapped = MapRows(fn=identity, input_dependencies=[original.dag])
    second = DeriveListFilesPushdown().apply(LogicalPlan(mapped, original.context))
    assert _footer_indexer(listing_of(first).file_indexer)._listing_hint is not None
    assert _footer_indexer(listing_of(second).file_indexer)._listing_hint is None
    assert _footer_indexer(listing_of(original).file_indexer)._listing_hint is None
    combined = DeriveListFilesPushdown().apply(
        LogicalPlan(Union(input_dependencies=[first.dag, second.dag]), original.context)
    )
    assert all(
        _footer_indexer(node.file_indexer)._listing_hint is None
        for node in combined.dag.post_order_iter()
        if isinstance(node, ListFiles)
    )
    assert _footer_indexer(listing_of(first).file_indexer)._listing_hint is not None


def test_layout_key_prepared_once_but_each_footer_certified(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import Mock

    from ray.data._internal.datasource_v2.formats.parquet import footer_reader as module

    hint = _footer_indexer(
        listing_of(DeriveListFilesPushdown().apply(plan(tmp_path))).file_indexer
    )._listing_hint
    prepare = Mock(wraps=module._schema_only_layout_key)
    certify = Mock(wraps=module.physical_schema_decline_reason)
    monkeypatch.setattr(module, "_schema_only_layout_key", prepare)
    monkeypatch.setattr(module, "physical_schema_decline_reason", certify)
    info = file(tmp_path)
    reader = FooterReader(fs.LocalFileSystem(), io_concurrency=4, listing_hint=hint)
    try:
        assert prepare.call_count == 1
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reader._read_and_chunk, [info] * 16))
        assert prepare.call_count == 1
        assert certify.call_count == 16
        assert all(
            result.schema_only_layout is reader._schema_only_layout
            for result in results
        )
        assert all(
            [i for run in result.row_groups for i in run.unit_ids] == [3]
            for result in results
        )
    finally:
        reader.pool.shutdown(wait=True)


def test_layout_preparation_failure_preserves_baseline(tmp_path, monkeypatch):
    from ray.data._internal.datasource_v2.formats.parquet import footer_reader as module

    def fail(*args):
        raise RuntimeError("optional layout serialization unavailable")

    hint = _footer_indexer(
        listing_of(DeriveListFilesPushdown().apply(plan(tmp_path))).file_indexer
    )._listing_hint
    monkeypatch.setattr(module, "_schema_only_layout_key", fail)
    ids, chunk = groups(file(tmp_path), None, hint)
    assert ids == [0, 1, 2, 3]
    assert chunk.schema_only_layout is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
