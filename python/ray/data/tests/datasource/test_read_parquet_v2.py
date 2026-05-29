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
    original_v2 = ctx.use_datasource_v2
    original_blocks_per_task = ctx.num_blocks_per_read_task
    try:
        yield ctx
    finally:
        ctx.use_datasource_v2 = original_v2
        ctx.num_blocks_per_read_task = original_blocks_per_task


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


def test_read_parquet_v2_max_bucket_size_scales_with_num_blocks_per_read_task(
    tmp_path, restore_ctx
):
    """The V2 listing partitioner's per-bucket cap is
    ``target_max_block_size * num_blocks_per_read_task``, so each read
    task emits roughly ``num_blocks_per_read_task`` output blocks (the
    knob amortizes task-launch overhead and lifts ``avg_outputs_per_task``
    above 1).
    """
    _write(tmp_path / "data.parquet", pa.table({"a": [1, 2, 3]}))

    restore_ctx.use_datasource_v2 = True
    restore_ctx.num_blocks_per_read_task = 4
    ds = ray.data.read_parquet(str(tmp_path))

    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op.file_partitioner, RoundRobinPartitioner)
    assert (
        list_files_op.file_partitioner._max_bucket_size
        == restore_ctx.target_max_block_size * 4
    )


def test_read_parquet_v2_infer_metadata_size_bytes(tmp_path, restore_ctx):
    """``ReadFiles.infer_metadata().size_bytes`` is the projection-aware
    in-memory size estimate built from planning-time sample data (average
    per-file on-disk bytes × ``PARQUET_ENCODING_RATIO_ESTIMATE_DEFAULT`` ×
    column-mass ratio × ``num_buckets``). Surfaces a non-None value so
    downstream consumers (hash-shuffle aggregator-memory sizing via
    ``_try_estimate_output_bytes`` -> ``_get_default_aggregator_ray_remote_args``)
    skip the conservative 1-GiB-per-aggregator fallback.
    """
    _write(tmp_path / "data.parquet", pa.table({"a": list(range(1024))}))

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), override_num_blocks=4)

    read_files_op = ds._logical_plan.dag
    assert isinstance(read_files_op, ReadFiles)
    # Planning-time sample data is captured on the op.
    assert read_files_op.sample_avg_file_size is not None
    assert read_files_op.sample_avg_file_size > 0
    assert read_files_op.sample_per_column_bytes is not None
    assert read_files_op.num_buckets == 4
    # The size estimate is materialized via infer_metadata.
    size = read_files_op.infer_metadata().size_bytes
    assert size is not None
    assert size > 0


def test_read_parquet_v2_infer_metadata_projection_aware(tmp_path, restore_ctx):
    """When projection-pushdown trims the scanner column list, the size
    estimate scales down by the projected columns' mass-fraction of the
    file. Two columns with very different sizes; projecting only the
    narrow one should yield a meaningfully smaller estimate than reading
    both.

    Simulates the post-projection-pushdown state via
    ``ReadFiles.apply_projection`` (the optimizer rule that fires at
    execution time).
    """
    # Wide string column dominates file mass; narrow int column is tiny.
    # Strings are made unique so dict-encoding can't compress them away
    # — keeps ``total_uncompressed_size`` reflective of real data size.
    n = 4096
    wide_values = [f"unique-{i}-" + ("x" * 256) for i in range(n)]
    narrow_values = list(range(n))
    _write(
        tmp_path / "data.parquet",
        pa.table({"narrow": narrow_values, "wide": wide_values}),
    )

    restore_ctx.use_datasource_v2 = True
    ds = ray.data.read_parquet(str(tmp_path), override_num_blocks=4)
    full_read = ds._logical_plan.dag
    assert isinstance(full_read, ReadFiles)
    full_size = full_read.infer_metadata().size_bytes

    # Simulate projection-pushdown applying the ``select_columns(["narrow"])``
    # rewrite into the scanner.
    narrow_read = full_read.apply_projection({"narrow": "narrow"})
    narrow_size = narrow_read.infer_metadata().size_bytes

    assert full_size is not None and full_size > 0
    assert narrow_size is not None and narrow_size > 0
    # Projecting only the narrow column should shrink the estimate
    # substantially — the wide string column dominates the file's
    # uncompressed bytes.
    assert (
        narrow_size < full_size / 2
    ), f"Expected projection-aware shrink: narrow={narrow_size}, full={full_size}"


def test_read_parquet_v2_size_bytes_propagates_through_chain(tmp_path, restore_ctx):
    """``size_bytes`` from ``ReadFiles.infer_metadata`` must propagate through
    map ops (Filter/Project) and shuffles (Join, Aggregate) so each
    downstream hash-shuffle sees a non-None size estimate and avoids the
    1 GiB-per-aggregator fallback. Walks ``Aggregate → Filter → Join →
    ReadFiles`` and asserts non-None ``size_bytes`` at every level.
    """
    from ray.data.expressions import col

    _write(
        tmp_path / "left.parquet",
        pa.table({"k": list(range(1024)), "v": list(range(1024))}),
    )
    right_dir = tmp_path / "right"
    right_dir.mkdir()
    _write(right_dir / "right.parquet", pa.table({"k": list(range(64))}))

    restore_ctx.use_datasource_v2 = True
    left = ray.data.read_parquet(str(tmp_path / "left.parquet"))
    right = ray.data.read_parquet(str(right_dir))

    joined = left.join(right, num_partitions=4, join_type="inner", on=("k",))
    filtered = joined.filter(expr=col("v") > 0)
    aggregated = filtered.groupby("k").count()

    dag = aggregated._logical_plan.dag
    # Walk the chain top-down through every logical op above the read,
    # asserting size_bytes is non-None at each level. Stop at ReadFiles —
    # its upstream ``ListFiles`` is a pure source (no size to report).
    seen_ops = []
    cursor = dag
    while True:
        seen_ops.append(type(cursor).__name__)
        assert (
            cursor.infer_metadata().size_bytes is not None
        ), f"size_bytes is None at {type(cursor).__name__} in chain {seen_ops}"
        if isinstance(cursor, ReadFiles):
            break
        deps = cursor.input_dependencies
        assert deps, f"unexpected leaf at {type(cursor).__name__}"
        cursor = deps[0]
    # Confirm we actually traversed all the op types we care about.
    assert {"Aggregate", "Filter", "Join", "ReadFiles"}.issubset(set(seen_ops))


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
    assert list_files_op.file_partitioner._num_buckets == 7
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
