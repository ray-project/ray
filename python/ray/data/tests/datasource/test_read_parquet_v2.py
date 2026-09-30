"""Integration-ish tests for ``read_parquet()`` on the DataSourceV2 path.

These tests exercise planning-time behavior: schema inference,
``ListFiles → ReadFiles`` attachment to the logical plan, and
unsupported-option gating. They call ``ray.data.read_parquet`` which
triggers Ray auto-init, so they live alongside the other datasource
integration tests rather than under ``tests/unit/``.
"""

import logging
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
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


def _rows(ds):
    return sorted(r["a"] for r in ds.take_all())


def test_read_parquet_v2_missing_path_raises_without_flag(tmp_path, restore_ctx):
    restore_ctx.use_datasource_v2 = True
    real = tmp_path / "a.parquet"
    _write(real, pa.table({"a": [1, 2]}))
    missing = str(tmp_path / "gone.parquet")

    with pytest.raises(FileNotFoundError):
        ray.data.read_parquet([str(real), missing]).take_all()


def test_read_parquet_v2_ignore_missing_paths(tmp_path, restore_ctx):
    restore_ctx.use_datasource_v2 = True
    real = tmp_path / "a.parquet"
    _write(real, pa.table({"a": [1, 2]}))
    missing = str(tmp_path / "gone.parquet")

    ds = ray.data.read_parquet([str(real), missing], ignore_missing_paths=True)
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_skip_paths_drops_existing_file(tmp_path, restore_ctx):
    restore_ctx.use_datasource_v2 = True
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    _write(b, pa.table({"a": [3, 4]}))

    ds = ray.data.read_parquet([str(a), str(b)], skip_paths=[str(b)])
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_skip_paths_accepts_single_string(tmp_path, restore_ctx):
    # ``skip_paths`` accepts a bare string (like ``paths``), not just a list.
    restore_ctx.use_datasource_v2 = True
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    _write(b, pa.table({"a": [3, 4]}))

    ds = ray.data.read_parquet([str(a), str(b)], skip_paths=str(b))
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_skip_paths_accepts_bare_pathlib_path(tmp_path, restore_ctx):
    # A bare pathlib.Path must not be treated as an iterable (it isn't one).
    restore_ctx.use_datasource_v2 = True
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    _write(b, pa.table({"a": [3, 4]}))

    ds = ray.data.read_parquet([str(a), str(b)], skip_paths=b)
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_skip_paths_drops_missing_without_ignore(tmp_path, restore_ctx):
    # ``skip_paths`` excludes a named path before the existence check, so a
    # missing entry is dropped even without ``ignore_missing_paths``.
    restore_ctx.use_datasource_v2 = True
    a = tmp_path / "a.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    missing = str(tmp_path / "gone.parquet")

    ds = ray.data.read_parquet([str(a), missing], skip_paths=[missing])
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_skip_paths_excludes_file_under_directory(
    tmp_path, restore_ctx
):
    restore_ctx.use_datasource_v2 = True
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    _write(b, pa.table({"a": [3, 4]}))

    ds = ray.data.read_parquet([str(tmp_path)], skip_paths=[str(a)])
    assert _rows(ds) == [3, 4]


def test_read_parquet_v1_rejects_new_params(tmp_path, restore_ctx):
    restore_ctx.use_datasource_v2 = False
    a = tmp_path / "a.parquet"
    _write(a, pa.table({"a": [1, 2]}))

    with pytest.raises(NotImplementedError, match="V2 datasource"):
        ray.data.read_parquet([str(a)], ignore_missing_paths=True)
    with pytest.raises(NotImplementedError, match="V2 datasource"):
        ray.data.read_parquet([str(a)], skip_paths=[str(a)])


def test_read_parquet_v1_empty_skip_paths_is_noop(tmp_path, restore_ctx):
    # An empty ``skip_paths`` requests nothing, so it must not trip the V1
    # guard even though V1 doesn't implement the parameter.
    restore_ctx.use_datasource_v2 = False
    a = tmp_path / "a.parquet"
    _write(a, pa.table({"a": [1, 2]}))

    ds = ray.data.read_parquet([str(a)], skip_paths=[])
    assert _rows(ds) == [1, 2]


def test_read_parquet_v2_uses_footer_indexer_with_bin_packer(tmp_path, restore_ctx):
    """With the footer indexer on, ``ListFiles`` pairs it with the bin packer.

    The packer sizes read units from footer stats, so ``override_num_blocks``
    no longer drives a partitioner bucket count for Parquet. The global
    ``DataContext`` must still not be mutated.
    """
    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )

    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    original = restore_ctx.read_op_min_num_blocks
    ds = ray.data.read_parquet(str(tmp_path), override_num_blocks=7)

    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op, ListFiles)
    assert isinstance(list_files_op.file_indexer, FooterFileIndexer)
    # The packer *is* the partitioner now: listing discovers row groups, the
    # partitioner groups them into read units.
    assert isinstance(list_files_op.file_partitioner, OnlineBinPacker)
    assert restore_ctx.read_op_min_num_blocks == original


def _physical_row_groups(directory):
    """``(path, rg_id)`` pairs and total rows from parquet footers on disk."""
    pairs = []
    total_rows = 0
    for path in sorted(directory.glob("*.parquet")):
        parquet_file = pq.ParquetFile(path)
        total_rows += parquet_file.metadata.num_rows
        pairs.extend((str(path), i) for i in range(parquet_file.num_row_groups))
    return pairs, total_rows


def _row_group_pairs(manifests):
    """Expand each listing/bin row into ``(path, rg_id)`` pairs plus row count."""
    pairs = []
    total_rows = 0
    for manifest in manifests:
        for path, md in zip(manifest.paths, manifest.file_chunk_metadatas):
            assert md is not None and "row_group_ids" in md, (
                "FooterFileIndexer must emit Parquet row-group metadata; "
                "without it OnlineBinPacker falls back to whole-file bins"
            )
            pairs.extend((str(path), int(rg_id)) for rg_id in md["row_group_ids"])
            total_rows += int(md["num_rows"])
    return sorted(pairs), total_rows


# 1 TiB: larger than any fixture row group, so coalescing merges a whole file
# and packing with this cap puts every run in one shared bin.
_UNLIMITED_BYTES = 1 << 40


@dataclass(frozen=True)
class _IndexerPackerCase:
    num_files: int
    rows_per_file: int
    row_group_size: int
    coalesce_bytes: int
    max_bin_bytes: int
    split_coalesced: bool
    expected_bins: int

    @property
    def row_groups_per_file(self) -> int:
        return self.rows_per_file // self.row_group_size

    @property
    def expected_listing_rows(self) -> int:
        # Uncoalesced: one listing row per physical row group.
        # Coalesced: one listing row per file.
        if self.coalesce_bytes:
            return self.num_files
        return self.num_files * self.row_groups_per_file


@pytest.mark.parametrize(
    "case",
    [
        # listing 12: f0:[0][1][2][3] f1:[0][1][2][3] f2:[0][1][2][3]
        # bins     1: [ f0:0-3 | f1:0-3 | f2:0-3 ]
        pytest.param(
            _IndexerPackerCase(
                num_files=3,
                rows_per_file=20,
                row_group_size=5,
                coalesce_bytes=0,
                max_bin_bytes=_UNLIMITED_BYTES,
                split_coalesced=False,
                expected_bins=1,
            ),
            id="uncoalesced-light-files-share-one-bin",
        ),
        # listing 8: f0:[0][1][2][3] f1:[0][1][2][3]
        # bins    8: [f0:0][f0:1][f0:2][f0:3] [f1:0][f1:1][f1:2][f1:3]
        pytest.param(
            _IndexerPackerCase(
                num_files=2,
                rows_per_file=20,
                row_group_size=5,  # in number of rows
                coalesce_bytes=0,
                max_bin_bytes=1,
                split_coalesced=False,
                expected_bins=8,
            ),
            id="uncoalesced-tiny-bin-one-per-row-group",
        ),
        # listing 2: f0:[0 1 2 3] f1:[0 1 2 3]
        # bins    1: [ f0:0-3 | f1:0-3 ]
        pytest.param(
            _IndexerPackerCase(
                num_files=2,
                rows_per_file=20,
                row_group_size=5,
                coalesce_bytes=_UNLIMITED_BYTES,
                max_bin_bytes=_UNLIMITED_BYTES,
                split_coalesced=False,
                expected_bins=1,
            ),
            id="coalesced-files-share-one-bin",
        ),
        # listing 2: f0:[0 1 2 3] f1:[0 1 2 3]
        # bins    2: [f0:0-3] [f1:0-3]
        pytest.param(
            _IndexerPackerCase(
                num_files=2,
                rows_per_file=20,
                row_group_size=5,
                coalesce_bytes=_UNLIMITED_BYTES,
                max_bin_bytes=1,
                split_coalesced=False,
                expected_bins=2,
            ),
            id="coalesced-tiny-bin-one-per-file",
        ),
        # listing 2: f0:[0 1 2 3] f1:[0 1 2 3]
        # bins    8: [f0:0][f0:1][f0:2][f0:3] [f1:0][f1:1][f1:2][f1:3]
        pytest.param(
            _IndexerPackerCase(
                num_files=2,
                rows_per_file=20,
                row_group_size=5,
                coalesce_bytes=_UNLIMITED_BYTES,
                max_bin_bytes=1,
                split_coalesced=True,
                expected_bins=8,
            ),
            id="split-coalesced-tiny-bin-one-per-row-group",
        ),
    ],
)
def test_footer_indexer_feeds_online_bin_packer(tmp_path, case: _IndexerPackerCase):
    """Listing rows from ``FooterFileIndexer`` are what ``OnlineBinPacker`` packs.

    The indexer emits one manifest row per row-group run; the packer groups
    those runs into read units. Coalescing happens in the indexer, splitting
    coalesced runs back at physical boundaries happens in the packer.
    """
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.common.online_bin_packer import (
        OnlineBinPacker,
    )

    _write_row_groups(
        tmp_path,
        num_files=case.num_files,
        rows_per_file=case.rows_per_file,
        row_group_size=case.row_group_size,
    )
    physical, physical_rows = _physical_row_groups(tmp_path)
    assert physical_rows == case.num_files * case.rows_per_file

    indexer = FooterFileIndexer(
        ignore_missing_paths=False,
        num_workers=1,
        coalesce_bytes=case.coalesce_bytes,
        footer_batch_size=2,
    )
    listed = list(
        indexer.list_files(
            pa.array([str(tmp_path)]),
            filesystem=LocalFileSystem(),
            preserve_order=True,
        )
    )
    listed_pairs, listed_rows = _row_group_pairs(listed)
    assert sum(len(manifest) for manifest in listed) == case.expected_listing_rows
    assert listed_pairs == physical
    assert listed_rows == physical_rows

    packer = OnlineBinPacker(case.max_bin_bytes, split_coalesced=case.split_coalesced)
    bins = []
    for manifest in listed:
        packer.add_input(manifest)
        while packer.has_partition():
            bins.append(packer.next_partition())
    packer.finalize()
    while packer.has_partition():
        bins.append(packer.next_partition())

    packed_pairs, packed_rows = _row_group_pairs(bins)
    assert len(bins) == case.expected_bins
    assert packed_pairs == physical
    assert packed_rows == physical_rows


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
    ``list_files``, so it footer-sweeps every file during listing -- the exact
    IO this rule exists to defer into the parallel count pass -- and emits one
    manifest row per row-group run, which would count each file once per run.
    """
    from ray.data._internal.datasource_v2.common.non_sampling_file_indexer import (
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

    Regression guard for over-counting: the footer indexer emits one manifest
    row per row-group run, so a multi-row-group file would otherwise be counted
    once per run.
    """
    _write_row_groups(
        tmp_path,
        num_files=num_files,
        rows_per_file=rows_per_file,
        row_group_size=row_group_size,
    )

    restore_ctx.use_datasource_v2 = True

    assert ray.data.read_parquet(str(tmp_path)).count() == num_files * rows_per_file


def test_count_pushdown_honors_skip_paths(tmp_path, restore_ctx):
    """The rebuilt indexer must keep ``skip_paths``, or the skipped file's rows
    are counted even though reading the dataset excludes them."""
    a = tmp_path / "a.parquet"
    b = tmp_path / "b.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    _write(b, pa.table({"a": [3, 4, 5]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet([str(a), str(b)], skip_paths=[str(b)])

    assert ds.count() == 2


def test_count_pushdown_skip_paths_tolerates_missing_path(tmp_path, restore_ctx):
    """``skip_paths`` drops a named path before the existence check, so a
    skipped-but-missing path must not fail a pushed-down ``count()``."""
    a = tmp_path / "a.parquet"
    _write(a, pa.table({"a": [1, 2]}))
    missing = str(tmp_path / "gone.parquet")

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet([str(a), missing], skip_paths=[missing])

    assert ds.count() == 2


def test_merge_schema_discovers_column_after_default_sample(tmp_path, restore_ctx):
    for i in range(17):
        data = {"id": [i]}
        if i == 16:
            data["new_feature"] = ["latest"]
        table = pa.table(data)
        if i == 16:
            table = table.cast(
                pa.schema(
                    [
                        pa.field("id", pa.int64()),
                        pa.field("new_feature", pa.string(), nullable=False),
                    ]
                )
            )
        _write(tmp_path / f"f{i:02d}.parquet", table)

    restore_ctx.use_datasource_v2 = True
    bounded = ray.data.read_parquet(str(tmp_path))
    merged = ray.data.read_parquet(str(tmp_path), merge_schema=True)

    assert "new_feature" not in bounded.schema().names
    assert "new_feature" in merged.schema().names
    list_files = merged._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files, ListFiles)
    assert sum(len(block) for block in list_files.prelisted_file_infos) == 17

    rows = sorted(merged.take_all(), key=lambda row: row["id"])
    assert all(row["new_feature"] is None for row in rows[:16])
    assert rows[16]["new_feature"] == "latest"


def test_merge_schema_warns_for_large_candidate_set(
    tmp_path, restore_ctx, monkeypatch, caplog
):
    import importlib

    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    _write(first, pa.table({"id": [1]}))
    _write(second, pa.table({"id": [2]}))

    restore_ctx.use_datasource_v2 = True
    read_api = importlib.import_module("ray.data.read_api")
    monkeypatch.setattr(read_api, "_FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT", 1)

    with caplog.at_level(logging.WARNING, logger="ray.data.read_api"):
        ray.data.read_parquet([str(first), str(second)], merge_schema=True)

    messages = [
        record.getMessage()
        for record in caplog.records
        if record.name == "ray.data.read_api"
        and "candidate files" in record.getMessage()
    ]
    assert len(messages) == 1
    assert "2 candidate files" in messages[0]


def test_merge_schema_null_fills_nested_struct_field(tmp_path, restore_ctx):
    old_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "nested",
                pa.struct([pa.field("a", pa.int64(), nullable=False)]),
                nullable=False,
            ),
        ]
    )
    new_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field(
                "nested",
                pa.struct(
                    [
                        pa.field("a", pa.int64(), nullable=False),
                        pa.field("late", pa.string(), nullable=True),
                    ]
                ),
                nullable=False,
            ),
        ]
    )
    _write(
        tmp_path / "a.parquet",
        pa.Table.from_pylist([{"id": 1, "nested": {"a": 1}}], schema=old_schema),
    )
    _write(
        tmp_path / "b.parquet",
        pa.Table.from_pylist(
            [{"id": 2, "nested": {"a": 2, "late": "value"}}],
            schema=new_schema,
        ),
    )

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)

    bounded = ray.data.read_parquet(str(tmp_path))
    assert ds.schema().base_schema == bounded.schema().base_schema
    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    assert rows[0]["nested"]["late"] is None
    assert rows[1]["nested"]["late"] == "value"


def test_merge_schema_fails_on_incompatible_file_outside_sample(tmp_path, restore_ctx):
    for i in range(16):
        _write(tmp_path / f"f{i:02d}.parquet", pa.table({"id": [i]}))
    _write(
        tmp_path / "f16.parquet",
        pa.table({"id": pa.array([["bad"]], type=pa.list_(pa.string()))}),
    )

    restore_ctx.use_datasource_v2 = True
    assert ray.data.read_parquet(str(tmp_path)).schema().names == ["id"]
    with pytest.raises(pa.ArrowTypeError, match="f16.parquet"):
        ray.data.read_parquet(str(tmp_path), merge_schema=True)


def test_merge_schema_prelisted_blocks_feed_one_listing_task(
    tmp_path, restore_ctx, monkeypatch
):
    from ray.data._internal.datasource_v2.common import listing_utils

    monkeypatch.setattr(listing_utils, "DEFAULT_SCHEMA_FILE_INFO_BLOCK_SIZE", 1)
    _write(tmp_path / "a.parquet", pa.table({"id": [1]}))
    _write(tmp_path / "b.parquet", pa.table({"id": [2], "later": [3]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)
    list_files = ds._logical_plan.dag.input_dependencies[0]

    assert len(list_files.prelisted_file_infos) == 2
    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    assert rows[0]["later"] is None
    assert rows[1]["later"] == 3


def test_merge_schema_prelisted_chunks_preserve_arrow_buffers(
    tmp_path, restore_ctx, monkeypatch
):
    from ray.data._internal.datasource_v2.common import listing_utils

    monkeypatch.setattr(listing_utils, "DEFAULT_SCHEMA_FILE_INFO_BLOCK_SIZE", 1)
    first = tmp_path / "a.parquet"
    second = tmp_path / "b.parquet"
    _write(first, pa.table({"id": [1]}))
    _write(second, pa.table({"id": [2], "later": [3]}))

    original_sample_files = listing_utils.sample_files
    discovered_file_info_tables = []

    def capture_discovered_file_infos(*args, **kwargs):
        manifest = original_sample_files(*args, **kwargs)
        if kwargs.get("max_files") is None:
            discovered_file_info_tables.append(
                manifest.as_block().select(["__path", "__file_size"])
            )
        return manifest

    monkeypatch.setattr(listing_utils, "sample_files", capture_discovered_file_infos)

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet([str(first), str(second)], merge_schema=True)
    list_files = ds._logical_plan.dag.input_dependencies[0]
    blocks = list_files.prelisted_file_infos
    source = discovered_file_info_tables[0]

    assert len(blocks) == 2
    assert all(block.schema == source.schema for block in blocks)
    assert [path for block in blocks for path in block["__path"].to_pylist()] == source[
        "__path"
    ].to_pylist()

    def buffer_addresses(table):
        return {
            buffer.address
            for column in table.columns
            for chunk in column.chunks
            for buffer in chunk.buffers()
            if buffer is not None
        }

    retained_buffer_addresses = set().union(
        *(buffer_addresses(block) for block in blocks)
    )
    assert buffer_addresses(source).isdisjoint(retained_buffer_addresses)


def test_merge_schema_prelisted_listing_skips_checkpointed_row_group(
    tmp_path, restore_ctx
):
    from pyarrow.fs import LocalFileSystem

    from ray.data._internal.datasource_v2.common.listing_utils import (
        list_files_for_each_block,
    )
    from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
    from ray.data._internal.datasource_v2.read_units import (
        EXCLUDED_READ_UNIT_IDS_KWARG_NAME,
    )
    from ray.data._internal.execution.interfaces.task_context import TaskContext

    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"id": [1, 2, 3, 4]}), path, row_group_size=2)

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(path), merge_schema=True)
    list_files = ds._logical_plan.dag.input_dependencies[0]
    context = TaskContext(
        task_idx=0,
        op_name="ListFiles",
        kwargs={EXCLUDED_READ_UNIT_IDS_KWARG_NAME: {f"{path}#rg0"}},
    )

    blocks = list(
        list_files_for_each_block(
            list_files.prelisted_file_infos,
            context,
            indexer=list_files.file_indexer,
            filesystem=LocalFileSystem(),
            prelisted_file_infos=True,
            preserve_order=True,
        )
    )
    remaining = [
        row_group_id
        for block in blocks
        for metadata in FileManifest(block).file_chunk_metadatas
        for row_group_id in metadata["row_group_ids"]
    ]

    assert remaining == [1]


def test_merge_schema_null_fills_with_extension_column(tmp_path, restore_ctx):
    import numpy as np

    if not hasattr(pa, "FixedShapeTensorArray"):
        pytest.skip("PyArrow fixed-shape tensor extension is unavailable")

    for i in range(17):
        data = {
            "id": pa.array([i]),
            "tensor": pa.FixedShapeTensorArray.from_numpy_ndarray(
                np.array([[i]], dtype=np.int64)
            ),
        }
        if i == 16:
            data["new_feature"] = pa.array(["latest"])
        _write(tmp_path / f"f{i:02d}.parquet", pa.table(data))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)

    assert "new_feature" in ds.schema().names
    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    assert all(row["new_feature"] is None for row in rows[:16])
    assert rows[16]["new_feature"] == "latest"


@pytest.mark.parametrize("tensor_kind", ["native", "ray_v2"])
def test_merge_schema_arrow_null_fills_new_extension_column(
    tmp_path, restore_ctx, tensor_kind
):
    import numpy as np

    for i in range(17):
        data = {"id": pa.array([i])}
        if i == 16:
            if tensor_kind == "native":
                if not hasattr(pa, "FixedShapeTensorArray"):
                    pytest.skip("PyArrow fixed-shape tensor extension is unavailable")
                data["tensor"] = pa.FixedShapeTensorArray.from_numpy_ndarray(
                    np.array([[i]], dtype=np.int64)
                )
            else:
                from ray.data._internal.tensor_extensions.arrow import ArrowTensorTypeV2

                data["tensor"] = pa.ExtensionArray.from_storage(
                    ArrowTensorTypeV2((1,), pa.int64()),
                    pa.array([[i]], type=pa.large_list(pa.int64())),
                )
        _write(tmp_path / f"f{i:02d}.parquet", pa.table(data))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)

    assert isinstance(
        ds.schema().base_schema.field("tensor").type, pa.BaseExtensionType
    )
    id_is_null = []
    for batch in ds.iter_batches(batch_format="pyarrow"):
        id_is_null.extend(
            zip(
                batch.column("id").to_pylist(),
                batch.column("tensor").is_null().to_pylist(),
            )
        )
    assert sorted(id_is_null) == [(i, i < 16) for i in range(17)]


def test_merge_schema_excludes_files_appended_after_planning(tmp_path, restore_ctx):
    _write(tmp_path / "a.parquet", pa.table({"id": [1]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)
    _write(tmp_path / "b.parquet", pa.table({"id": [2], "later": [3]}))

    assert ds.schema().names == ["id"]
    assert [row["id"] for row in ds.take_all()] == [1]
    assert ds.count() == 1


def test_merge_schema_applies_initial_path_filters(tmp_path, restore_ctx):
    keep = tmp_path / "keep.parquet"
    skip = tmp_path / "skip.parquet"
    missing = tmp_path / "missing.parquet"
    _write(keep, pa.table({"id": [1]}))
    _write(skip, pa.table({"id": [2], "skipped_column": [3]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(
        [str(keep), str(skip), str(missing)],
        merge_schema=True,
        skip_paths=[str(skip)],
        ignore_missing_paths=True,
    )

    assert ds.schema().names == ["id"]
    assert [row["id"] for row in ds.take_all()] == [1]


def test_merge_schema_partition_filtered_count(tmp_path, restore_ctx):
    from ray.data.expressions import col

    for year, count in (("2024", 2), ("2025", 3)):
        part = tmp_path / f"year={year}"
        part.mkdir()
        _write(part / "data.parquet", pa.table({"id": list(range(count))}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)
    assert ds.count() == 5
    assert ds.filter(expr=col("year") == "2025").count() == 3


def test_merge_schema_lineage_retains_candidates(tmp_path, restore_ctx):
    _write(tmp_path / "a.parquet", pa.table({"id": [1]}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), merge_schema=True)
    restored = ray.data.Dataset.deserialize_lineage(ds.serialize_lineage())
    _write(tmp_path / "b.parquet", pa.table({"id": [2]}))

    assert [row["id"] for row in restored.take_all()] == [1]


def test_merge_schema_rejects_schema_override_and_v1(tmp_path, restore_ctx):
    path = tmp_path / "a.parquet"
    _write(path, pa.table({"id": [1]}))

    restore_ctx.use_datasource_v2 = True
    with pytest.raises(ValueError, match="cannot be combined"):
        ray.data.read_parquet(
            str(path), schema=pa.schema([("id", pa.int64())]), merge_schema=True
        )

    restore_ctx.use_datasource_v2 = False
    with pytest.raises(NotImplementedError, match="requires the V2 datasource"):
        ray.data.read_parquet(str(path), merge_schema=True)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
