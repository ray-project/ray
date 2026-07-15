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


# --- Listing-input sharding ---------------------------------------------------


def _mk_multi_path_list_files(tmp_path, num_files: int, shuffle_seed=None):
    """A ListFiles op pointing at ``num_files`` explicit parquet file paths."""
    import pyarrow.fs as pafs

    paths = []
    for i in range(num_files):
        p = str(tmp_path / f"f{i:04d}.parquet")
        pq.write_table(pa.table({"x": [i]}), p)
        paths.append(p)

    def _shuffle_factory():
        return None if shuffle_seed is None else FileShuffleConfig(seed=shuffle_seed)

    return ListFiles(
        paths=paths,
        file_indexer=_mk_indexer(),
        filesystem=pafs.LocalFileSystem(),
        source_paths=paths,
        shuffle_config_factory=_shuffle_factory,
    )


def _bundle_paths(buffer):
    import ray

    out = []
    for ref_bundle in buffer._input_data:
        for entry in ref_bundle.blocks:
            out += ray.get(entry.ref)[PATH_COLUMN_NAME].to_pylist()
    return out


@pytest.mark.parametrize("num_files", [1, 2, 50, 250])
def test_raw_paths_shard_across_tasks(ray_start_2_cpus_shared, tmp_path, num_files):
    # Raw input paths are sharded across listing bundles (one Ray task each),
    # capped at DEFAULT_MAX_NUM_LIST_FILES_TASKS.
    from ray.data._internal.planner.plan_list_files_op import (
        DEFAULT_MAX_NUM_LIST_FILES_TASKS,
        _create_input_data_buffer,
    )
    from ray.data.context import DataContext

    op = _mk_multi_path_list_files(tmp_path, num_files=num_files)
    buffer = _create_input_data_buffer(
        op, DataContext.get_current(), should_parallelize=True
    )

    expected = min(DEFAULT_MAX_NUM_LIST_FILES_TASKS, num_files)
    assert len(buffer._input_data) == expected

    # Every input path is sharded exactly once across all bundles.
    paths = _bundle_paths(buffer)
    assert len(paths) == num_files
    assert len(set(paths)) == num_files


def test_shuffle_forces_single_bundle_with_all_paths(ray_start_2_cpus_shared, tmp_path):
    # Shuffle needs one global permutation, so listing stays a single task
    # containing every input path.
    from ray.data._internal.planner.plan_list_files_op import (
        _create_input_data_buffer,
    )
    from ray.data.context import DataContext

    op = _mk_multi_path_list_files(tmp_path, num_files=30, shuffle_seed=7)
    buffer = _create_input_data_buffer(
        op, DataContext.get_current(), should_parallelize=False
    )
    assert len(buffer._input_data) == 1
    assert len(_bundle_paths(buffer)) == 30


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
