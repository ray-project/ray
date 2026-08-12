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
from ray.data._internal.datasource_v2.partitioners.round_robin_partitioner import (
    RoundRobinPartitioner,
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


def test_read_parquet_v2_override_num_blocks_drives_partitioner(tmp_path, restore_ctx):
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    original = restore_ctx.read_op_min_num_blocks
    ds = ray.data.read_parquet(str(tmp_path), override_num_blocks=7)

    # The override should drive the ListFiles partitioner's bucket count
    # for this read only — the global DataContext must not be mutated.
    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op, ListFiles)
    assert isinstance(list_files_op.file_partitioner, RoundRobinPartitioner)
    assert list_files_op.file_partitioner.num_buckets == 7
    assert restore_ctx.read_op_min_num_blocks == original


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
