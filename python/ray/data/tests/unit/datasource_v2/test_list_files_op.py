"""Unit tests for :class:`ListFiles` logical op.

Full physical-planning tests live in the CI parquet regression suite
(they need Ray initialized for ``ray.put`` on the listing input
bundles). Here we exercise just the logical op shape and the shuffle
factory semantics.
"""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ray.data._internal.datasource_v2.listing.file_indexer import (
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
)
from ray.data._internal.logical.operators import ListFiles
from ray.data.datasource.file_based_datasource import FileShuffleConfig


def _mk_indexer():
    return NonSamplingFileIndexer(ignore_missing_paths=False)


def _mk_list_files(tmp_path, num_files: int = 3, shuffle_seed=None):
    for i in range(num_files):
        pq.write_table(pa.table({"x": [i]}), str(tmp_path / f"f{i}.parquet"))
    paths = [str(tmp_path / f"f{i}.parquet") for i in range(num_files)]

    def _shuffle_factory():
        if shuffle_seed is None:
            return None
        return FileShuffleConfig(seed=shuffle_seed)

    import pyarrow.fs as pafs

    return ListFiles(
        paths=paths,
        file_indexer=_mk_indexer(),
        filesystem=pafs.LocalFileSystem(),
        source_paths=paths,
        shuffle_config_factory=_shuffle_factory,
    )


def test_list_files_infers_manifest_schema(tmp_path):
    op = _mk_list_files(tmp_path, num_files=1)
    schema = op.infer_schema()
    assert schema.names == [PATH_COLUMN_NAME, FILE_SIZE_COLUMN_NAME]
    assert schema.field(PATH_COLUMN_NAME).type == pa.string()
    assert schema.field(FILE_SIZE_COLUMN_NAME).type == pa.int64()


def test_list_files_has_no_input_dependencies(tmp_path):
    op = _mk_list_files(tmp_path, num_files=1)
    assert op.input_dependencies == []
    assert op.num_outputs is None
    assert op.output_data() is None


def test_shuffle_config_factory_none_when_unconfigured(tmp_path):
    op = _mk_list_files(tmp_path, num_files=1, shuffle_seed=None)
    assert op.shuffle_config_factory() is None


def test_shuffle_config_factory_returns_config_when_seeded(tmp_path):
    op = _mk_list_files(tmp_path, num_files=1, shuffle_seed=42)
    config = op.shuffle_config_factory()
    assert isinstance(config, FileShuffleConfig)
    assert config.seed == 42


# --- Plan-time path expansion / listing-task fan-out --------------------------


def _mk_prefix_list_files(tmp_path, num_files: int, shuffle_seed=None):
    """A ListFiles op pointing at a SINGLE directory prefix containing
    ``num_files`` parquet files -- the case that used to collapse to one task."""
    import pyarrow.fs as pafs

    for i in range(num_files):
        pq.write_table(pa.table({"x": [i]}), str(tmp_path / f"f{i:04d}.parquet"))

    def _shuffle_factory():
        return None if shuffle_seed is None else FileShuffleConfig(seed=shuffle_seed)

    return ListFiles(
        paths=[str(tmp_path)],  # ONE prefix
        file_indexer=_mk_indexer(),
        filesystem=pafs.LocalFileSystem(),
        source_paths=[str(tmp_path)],
        shuffle_config_factory=_shuffle_factory,
    )


def _bundle_paths(buffer):
    import ray

    out = []
    for ref_bundle in buffer._input_data:
        for entry in ref_bundle.blocks:
            out += ray.get(entry.ref)[PATH_COLUMN_NAME].to_pylist()
    return out


def test_expand_paths_to_files_lists_and_sorts(tmp_path):
    # Ray-free: expanding a directory prefix returns every concrete file,
    # path-sorted, without reading footers.
    import pyarrow.fs as pafs

    from ray.data._internal.planner.plan_list_files_op import (
        _expand_paths_to_files,
    )

    for i in range(5):
        pq.write_table(pa.table({"x": [i]}), str(tmp_path / f"f{i}.parquet"))

    files = _expand_paths_to_files(
        [str(tmp_path)], pafs.LocalFileSystem(), False, num_workers=4
    )
    assert len(files) == 5
    assert files == sorted(files)
    assert all(p.endswith(".parquet") for p in files)


@pytest.mark.parametrize("num_files", [1, 2, 50, 250])
def test_single_prefix_fans_out_across_tasks(
    ray_start_2_cpus_shared, tmp_path, num_files
):
    # THE regression: a single-prefix input with many files must shard into
    # many listing bundles (one Ray task each), not one. Pre-fix this was
    # always 1 bundle because sharding keyed on input-path count (== 1).
    from ray.data._internal.planner.plan_list_files_op import (
        DEFAULT_MAX_NUM_LIST_FILES_TASKS,
        _create_input_data_buffer,
    )
    from ray.data.context import DataContext

    op = _mk_prefix_list_files(tmp_path, num_files=num_files)
    buffer = _create_input_data_buffer(
        op, DataContext.get_current(), should_parallelize=True
    )

    expected = min(DEFAULT_MAX_NUM_LIST_FILES_TASKS, num_files)
    assert len(buffer._input_data) == expected

    # Files are partitioned exactly once across all bundles (affinity-safe).
    paths = _bundle_paths(buffer)
    assert len(paths) == num_files
    assert len(set(paths)) == num_files


def test_shuffle_forces_single_bundle_with_all_files(ray_start_2_cpus_shared, tmp_path):
    # Shuffle needs one global permutation, so listing stays a single task
    # containing every file.
    from ray.data._internal.planner.plan_list_files_op import (
        _create_input_data_buffer,
    )
    from ray.data.context import DataContext

    op = _mk_prefix_list_files(tmp_path, num_files=30, shuffle_seed=7)
    buffer = _create_input_data_buffer(
        op, DataContext.get_current(), should_parallelize=False
    )
    assert len(buffer._input_data) == 1
    assert len(_bundle_paths(buffer)) == 30


def test_kill_switch_reverts_to_raw_path_sharding(
    ray_start_2_cpus_shared, tmp_path, restore_data_context
):
    # With expansion disabled, a single prefix shards to a single bundle
    # (pre-fix behavior) -- the instant-rollback escape hatch.
    from ray.data._internal.planner.plan_list_files_op import (
        _create_input_data_buffer,
    )
    from ray.data.context import DataContext

    ctx = DataContext.get_current()
    ctx.list_files_expand_paths = False
    op = _mk_prefix_list_files(tmp_path, num_files=30)
    buffer = _create_input_data_buffer(op, ctx, should_parallelize=True)
    assert len(buffer._input_data) == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
