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


def test_list_files_ray_remote_args_default_to_empty(tmp_path):
    from dataclasses import replace

    op = _mk_list_files(tmp_path, num_files=1)
    assert op.ray_remote_args == {}

    scheduled = replace(op, ray_remote_args={"runtime_env": {"env_vars": {"A": "1"}}})
    assert scheduled.ray_remote_args == {"runtime_env": {"env_vars": {"A": "1"}}}


def test_indexer_requires_file_io_follows_its_chunker():
    from ray.data._internal.datasource_v2.chunkers.file_chunker import (
        WholeFileChunker,
    )

    class HeaderReadingChunker(WholeFileChunker):
        @property
        def requires_file_io(self) -> bool:
            return True

    assert not NonSamplingFileIndexer(ignore_missing_paths=False).requires_file_io
    assert NonSamplingFileIndexer(
        ignore_missing_paths=False, file_chunker=HeaderReadingChunker()
    ).requires_file_io


def test_partition_filter_cache_is_bounded_and_frozen():
    from unittest.mock import MagicMock

    from ray.data._internal.datasource_v2.listing.listing_utils import (
        _CachedPathPartitionFilter,
    )

    delegate = MagicMock()
    delegate.apply.return_value = False
    cache = _CachedPathPartitionFilter(delegate, max_cached_paths=2)
    for index in range(100):
        assert cache.apply(str(index)) is False
    assert len(cache._decisions) == 2
    cache.freeze()
    calls = delegate.apply.call_count
    assert cache.apply("0") is False
    assert delegate.apply.call_count == calls
    for index in range(100, 200):
        assert cache.apply(str(index)) is False
    assert len(cache._decisions) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-xvs"]))
