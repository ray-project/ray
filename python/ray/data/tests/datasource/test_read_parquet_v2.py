"""Integration-ish tests for ``read_parquet()`` on the DataSourceV2 path.

These tests exercise planning-time behavior: schema inference,
``ListFiles → ReadFiles`` attachment to the logical plan, and
unsupported-option gating. They call ``ray.data.read_parquet`` which
triggers Ray auto-init, so they live alongside the other datasource
integration tests rather than under ``tests/unit/``.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.listing.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.scanners.parquet_scanner import ParquetScanner
from ray.data._internal.logical.operators import ListFiles, ReadFiles
from ray.data.context import DataContext


def _write(path, table):
    pq.write_table(table, str(path))


@pytest.fixture
def restore_ctx():
    ctx = DataContext.get_current()
    original = ctx.use_datasource_v2
    try:
        yield ctx
    finally:
        ctx.use_datasource_v2 = original


def test_v2_flag_default():
    # The default is driven by ``DEFAULT_USE_DATASOURCE_V2``. Asserting
    # either direction here would be brittle, so just check that the
    # default is a bool.
    ctx = DataContext()
    assert isinstance(ctx.use_datasource_v2, bool)


def test_read_parquet_builds_list_files_read_files_chain(tmp_path, restore_ctx):
    f = tmp_path / "data.parquet"
    _write(f, pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path))

    assert isinstance(ds._logical_plan.dag, ReadFiles)
    assert isinstance(ds._logical_plan.dag.input_dependencies[0], ListFiles)
    schema = ds.schema()
    assert schema is not None
    assert "a" in schema.names
    assert "b" in schema.names


def test_read_parquet_v2_hive_partitioned(tmp_path, restore_ctx):
    for p in ["a", "b"]:
        d = tmp_path / f"color={p}"
        d.mkdir()
        _write(d / "data.parquet", pa.table({"x": [1, 2]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path))
    schema = ds.schema()
    assert "x" in schema.names
    assert "color" in schema.names


def test_read_parquet_v2_include_paths(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), include_paths=True)
    schema = ds.schema()
    assert "path" in schema.names


def test_read_parquet_v2_include_row_hash(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), include_row_hash=True)
    schema = ds.schema()
    assert schema is not None
    assert "row_hash" in schema.names
    assert schema.types[schema.names.index("row_hash")] == pa.uint64()


def test_read_parquet_v2_columns_applies_select_columns(tmp_path, restore_ctx):
    from ray.data._internal.logical.operators.map_operator import Project

    _write(tmp_path / "data.parquet", pa.table({"a": [1], "b": [2]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.warns(DeprecationWarning, match="`columns=` on `read_parquet`"):
        ds = ray.data.read_parquet(str(tmp_path), columns=["a"])

    # ``columns=`` is applied via ``ds.select_columns([...])``, which
    # wraps the ReadFiles op in a Project node.
    dag = ds._logical_plan.dag
    assert isinstance(dag, Project)
    assert [expr.name for expr in dag.exprs] == ["a"]
    assert isinstance(dag.input_dependencies[0], ReadFiles)


def test_read_parquet_v2_columns_with_include_paths_preserves_path(
    tmp_path, restore_ctx
):
    from ray.data._internal.logical.operators.map_operator import Project

    _write(tmp_path / "data.parquet", pa.table({"a": [1], "b": [2]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.warns(DeprecationWarning, match="`columns=` on `read_parquet`"):
        ds = ray.data.read_parquet(str(tmp_path), columns=["a"], include_paths=True)

    dag = ds._logical_plan.dag
    assert isinstance(dag, Project)
    # V1 ``columns=[...]`` retained ``"path"`` implicitly when
    # ``include_paths=True``; the V2 path appends it to keep that
    # behavior.
    assert [expr.name for expr in dag.exprs] == ["a", "path"]


def test_read_parquet_v2_uses_footer_indexer_without_partitioner(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    original = restore_ctx.read_op_min_num_blocks
    ds = ray.data.read_parquet(str(tmp_path), override_num_blocks=7)

    # Parquet V2 uses the footer indexer, which bin-packs read units itself, so
    # ``ListFiles`` carries no size-balancing partitioner (``override_num_blocks``
    # no longer drives a partitioner bucket count for Parquet). The global
    # DataContext must not be mutated.
    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op, ListFiles)
    assert isinstance(list_files_op.file_indexer, FooterFileIndexer)
    assert list_files_op.file_partitioner is None
    assert restore_ctx.read_op_min_num_blocks == original


def _write_row_groups(path, *, num_files, rows_per_file, row_group_size):
    """Write ``num_files`` parquet files, each split into several row groups."""
    for i in range(num_files):
        pq.write_table(
            pa.table({"a": list(range(rows_per_file))}),
            str(path / f"f{i}.parquet"),
            row_group_size=row_group_size,
        )


def _optimized_count_plan(ds):
    """The plan ``Dataset.count()`` executes, without executing it.

    ``Dataset.count()`` builds this internally and never exposes it, so tests
    reconstruct it to assert on the optimizer's output.
    """
    from ray.data._internal.logical.interfaces import LogicalPlan
    from ray.data._internal.logical.operators.count_operator import Count
    from ray.data._internal.logical.operators.map_operator import Project
    from ray.data._internal.logical.optimizers import LogicalOptimizer

    count_op = Count(
        input_dependencies=[
            Project(exprs=[], input_dependencies=[ds._logical_plan.dag])
        ]
    )
    return LogicalOptimizer().optimize(LogicalPlan(count_op, ds.context))


def _walk(op):
    yield op
    for child in op.input_dependencies:
        yield from _walk(child)


def test_count_pushdown_replaces_footer_indexer(tmp_path, restore_ctx):
    """``count()`` must not run the footer indexer.

    ``FooterFileIndexer`` subclasses ``NonSamplingFileIndexer`` but overrides
    ``list_files``, so it footer-sweeps every file during listing and bin-packs
    them into read units -- for a zero-column count projection every row group
    measures 0 bytes, collapsing the whole dataset into a single manifest and
    therefore a single count task.
    """
    from ray.data._internal.datasource_v2.chunkers.file_chunker import WholeFileChunker
    from ray.data._internal.datasource_v2.listing.file_indexer import (
        NonSamplingFileIndexer,
    )
    from ray.data._internal.logical.operators.map_operator import MapBatches

    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path))
    list_files_before = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_before, ListFiles)
    assert isinstance(list_files_before.file_indexer, FooterFileIndexer)

    dag = _optimized_count_plan(ds).dag

    assert isinstance(dag, MapBatches)
    assert not any(isinstance(op, ReadFiles) for op in _walk(dag))

    (list_files_op,) = [op for op in _walk(dag) if isinstance(op, ListFiles)]
    # Exact type: an ``isinstance`` check is precisely what let the footer
    # indexer through before.
    assert type(list_files_op.file_indexer) is NonSamplingFileIndexer
    assert isinstance(list_files_op.file_indexer.file_chunker, WholeFileChunker)
    assert list_files_op.file_partitioner is None


@pytest.mark.parametrize(
    "read_kwargs", [{}, {"include_paths": True}], ids=["plain", "include_paths"]
)
def test_count_pushdown_preserves_list_files_fields(tmp_path, restore_ctx, read_kwargs):
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), **read_kwargs)
    original = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(original, ListFiles)

    (rebuilt,) = [
        op for op in _walk(_optimized_count_plan(ds).dag) if isinstance(op, ListFiles)
    ]

    assert rebuilt.paths == original.paths
    assert rebuilt.source_paths == original.source_paths
    assert rebuilt.file_extensions == original.file_extensions
    assert rebuilt.partition_filter is original.partition_filter


@pytest.mark.parametrize("case", ["predicate", "limit"])
def test_count_pushdown_declines_row_reducing_reads(tmp_path, restore_ctx, case):
    """A row-reducing pushdown makes footer ``num_rows`` an overcount."""
    from ray.data.expressions import col

    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3, 4]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path))
    ds = ds.filter(expr=col("a") > 2) if case == "predicate" else ds.limit(2)

    assert any(isinstance(op, ReadFiles) for op in _walk(_optimized_count_plan(ds).dag))


@pytest.mark.parametrize(
    "num_files,rows_per_file,row_group_size",
    [(1, 10, None), (3, 100, 10), (4, 1000, 250)],
    ids=["single_file", "multi_file_many_row_groups", "multi_file_large"],
)
def test_count_matches_rows(
    tmp_path, restore_ctx, num_files, rows_per_file, row_group_size
):
    """End-to-end count correctness, including multi-row-group files.

    Regression guard for over-counting: the bin packer emits one manifest row
    per path *per bin*, so a file whose row groups span bins would otherwise be
    counted once per bin.
    """
    _write_row_groups(
        tmp_path,
        num_files=num_files,
        rows_per_file=rows_per_file,
        row_group_size=row_group_size,
    )

    restore_ctx.use_datasource_v2 = True

    assert ray.data.read_parquet(str(tmp_path)).count() == num_files * rows_per_file


def test_read_parquet_v2_filter_raises(tmp_path, restore_ctx):
    import pyarrow.dataset as pds

    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.raises(ValueError, match="`filter=` on `read_parquet`"):
        ray.data.read_parquet(str(tmp_path), filter=pds.field("a") > 1)


def test_read_parquet_v2_dataset_kwargs_rejects_partitioning(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.warns(DeprecationWarning, match="`dataset_kwargs`"):
        with pytest.raises(
            ValueError, match="'partitioning' parameter isn't supported"
        ):
            ray.data.read_parquet(
                str(tmp_path), dataset_kwargs={"partitioning": "hive"}
            )


def test_read_parquet_v2_dataset_kwargs_rejects_filters(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.warns(DeprecationWarning, match="`dataset_kwargs`"):
        with pytest.raises(ValueError, match="Row filtering via 'filters'"):
            ray.data.read_parquet(
                str(tmp_path), dataset_kwargs={"filters": [("a", ">", 0)]}
            )


def test_read_parquet_v2_dataset_kwargs_threads_through_to_scanner(
    tmp_path, restore_ctx
):
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.warns(DeprecationWarning, match="`dataset_kwargs`"):
        ds = ray.data.read_parquet(
            str(tmp_path),
            dataset_kwargs={
                "coerce_int96_timestamp_unit": "ms",
                "read_dictionary": ["a"],
            },
        )

    # ``read_dictionary`` is renamed to ``dictionary_columns`` to match
    # ``pds.ParquetFileFormat``; ``coerce_int96_timestamp_unit`` passes
    # through unchanged.
    read_files_op = ds._logical_plan.dag
    assert isinstance(read_files_op, ReadFiles)
    assert isinstance(read_files_op.scanner, ParquetScanner)
    assert read_files_op.scanner.parquet_format_kwargs == {
        "coerce_int96_timestamp_unit": "ms",
        "dictionary_columns": ["a"],
    }


def test_read_parquet_v2_empty_dir_raises(tmp_path, restore_ctx):
    restore_ctx.use_datasource_v2 = True
    with pytest.raises(ValueError, match="no files found"):
        ray.data.read_parquet(str(tmp_path))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
