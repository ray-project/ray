"""Single-query HiveServer2 datasource for the Ray Data V2 read path."""

from typing import AbstractSet, Iterable, Iterator, List, Optional

import pyarrow as pa

from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import (
    infer_table_schema,
    read_hs2_batches,
)
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    DataSourceWithMetadata,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner

_READ_UNIT = "hive://read"


class _HiveIndexer(FileIndexer):
    """Emit one manifest entry for the complete table or query read.

    Listing doesn't connect to HiveServer2. This entry represents the whole
    query, not a split. Future parallel table reads must describe each disjoint
    split in its manifest unit so the reader can issue the corresponding query.
    """

    def list_files(
        self,
        paths,
        *,
        filesystem=None,
        pruners=None,
        preserve_order=False,
        predicate=None,
        limit=None,
        projected_columns=None,
        shuffle_config=None,
        execution_idx=0,
        excluded_read_unit_ids: Optional[AbstractSet[str]] = None,
    ) -> Iterable[FileManifest]:
        if _READ_UNIT not in (excluded_read_unit_ids or ()):
            yield FileManifest.construct_manifest(
                paths=[_READ_UNIT], sizes=[0], chunk_metadatas=[None]
            )

    def list_file_infos(
        self, paths, *, filesystem=None, pruners=None, preserve_order=False
    ) -> Iterable[FileInfo]:
        yield FileInfo(path=_READ_UNIT, size=0)


class _HiveReader(Reader[FileManifest]):
    def __init__(self, spec: HiveReadSpec, schema: pa.Schema):
        self._spec = spec
        self._schema = schema

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        if len(input_split) != 1 or input_split.paths[0] != _READ_UNIT:
            raise ValueError("HiveServer2 requires exactly one read unit")
        yield from read_hs2_batches(self._spec, self._schema)


class _HiveScanner(Scanner[FileManifest]):
    def __init__(self, spec: HiveReadSpec, schema: pa.Schema):
        self._spec = spec
        self._schema = schema

    def read_schema(self) -> pa.Schema:
        return self._schema

    def create_reader(self) -> _HiveReader:
        return _HiveReader(self._spec, self._schema)


class HiveDatasourceV2(DataSourceWithMetadata[FileManifest]):
    """Read a table or trusted query through one data operation per execution."""

    def __init__(self, spec: HiveReadSpec):
        super().__init__("Hive", DatasourceCategory.DATABASE)
        self._spec = spec

    @property
    def paths(self) -> List[str]:
        return [_READ_UNIT]

    def _get_file_indexer(self) -> FileIndexer:
        return _HiveIndexer()

    def get_file_partitioner(self, *, hints=None):
        return None

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        if self._spec.schema is not None:
            return self._spec.schema
        return infer_table_schema(self._spec)

    def create_scanner(self, schema: pa.Schema, filesystem=None, **options) -> Scanner:
        return _HiveScanner(self._spec, schema)
