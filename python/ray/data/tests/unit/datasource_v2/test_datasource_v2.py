"""Contract tests for ``DataSourceV2`` and its two subclasses."""

from typing import List

import pyarrow as pa
import pyarrow.fs as pafs
import pytest

from ray.data._internal.datasource_v2.datasource_v2 import (
    DatasourceCategory,
    DataSourceV2,
    DataSourceWithMetadata,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.listing.file_indexer import (
    FileIndexer,
    NonSamplingFileIndexer,
)
from ray.data._internal.datasource_v2.parquet_datasource_v2 import ParquetDatasourceV2
from ray.data._internal.datasource_v2.scanners.scanner import Scanner
from ray.data.read_api import _read_datasource_v2

_SCHEMA = pa.schema([("x", pa.int64())])

# The auto-init wrapper on ``_read_datasource_v2`` would start Ray; the planning
# code underneath it does not need a cluster.
_plan_read = _read_datasource_v2.__wrapped__  # pyrefly: ignore[missing-attribute]


class _RecordingScanner(Scanner):
    """Minimal scanner that remembers the filesystem it was created with."""

    def __init__(self, filesystem):
        self.filesystem = filesystem

    def read_schema(self) -> pa.Schema:
        return _SCHEMA

    def create_reader(self):
        raise NotImplementedError


class _Members:
    """The abstract ``DataSourceV2`` members shared by the fakes below,
    except ``schema_needs_file_sample`` which each fake decides itself."""

    @property
    def paths(self) -> List[str]:
        return ["fake://table"]

    def _get_file_indexer(self) -> FileIndexer:
        return NonSamplingFileIndexer(ignore_missing_paths=False)

    def infer_schema(self, sample) -> pa.Schema:
        return _SCHEMA

    def create_scanner(self, schema, filesystem=None, **options) -> Scanner:
        self.scanner = _RecordingScanner(filesystem)
        return self.scanner


class _MetadataSource(_Members, DataSourceWithMetadata):
    """Implements exactly the abstract members of ``DataSourceWithMetadata``."""

    def __init__(self):
        super().__init__("table", DatasourceCategory.DATA_LAKE)


class _FileSource(_Members, FileDataSourceV2):
    def __init__(self):
        super().__init__("files", DatasourceCategory.FILE_BASED)

    @property
    def filesystem(self) -> pafs.FileSystem:
        return pafs.LocalFileSystem()

    @property
    def schema_needs_file_sample(self) -> bool:
        return False


class _BareSource(_Members, DataSourceV2):
    """Extends the base directly, which the read path rejects."""

    def __init__(self):
        super().__init__("bare", DatasourceCategory.DATABASE)

    @property
    def schema_needs_file_sample(self) -> bool:
        return False


def test_table_source_has_no_filesystem_members():
    source = _MetadataSource()
    for member in ("filesystem", "file_extensions", "shuffle"):
        assert not hasattr(source, member), member
    assert source.schema_needs_file_sample is False


def test_file_source_requires_filesystem():
    class _NoFilesystem(_Members, FileDataSourceV2):
        def __init__(self):
            super().__init__("broken", DatasourceCategory.FILE_BASED)

        @property
        def schema_needs_file_sample(self) -> bool:
            return False

    with pytest.raises(TypeError, match="filesystem"):
        _NoFilesystem()  # pyrefly: ignore[bad-instantiation]


def test_file_source_defaults():
    source = _FileSource()
    assert isinstance(source, DataSourceV2)
    assert source.file_extensions is None
    assert source.shuffle is None


def test_parquet_is_a_file_source():
    assert issubclass(ParquetDatasourceV2, FileDataSourceV2)


def test_read_passes_none_for_table_source():
    source = _MetadataSource()
    ds = _plan_read(source)
    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert list_files_op.filesystem is None
    assert list_files_op.file_extensions is None
    assert list_files_op.shuffle_config_factory() is None
    assert source.scanner.filesystem is None


def test_read_forwards_file_source_members():
    source = _FileSource()
    ds = _plan_read(source)
    list_files_op = ds._logical_plan.dag.input_dependencies[0]
    assert isinstance(list_files_op.filesystem, pafs.LocalFileSystem)
    assert isinstance(source.scanner.filesystem, pafs.LocalFileSystem)


def test_read_rejects_direct_base_subclass():
    with pytest.raises(TypeError, match="FileDataSourceV2.*DataSourceWithMetadata"):
        _plan_read(_BareSource())


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
