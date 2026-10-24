"""Single-query HiveServer2 datasource for the Ray Data V2 read path."""

from dataclasses import dataclass, field, replace
from typing import AbstractSet, Iterable, Iterator, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource_v2.common.pushdown_utils import combine_predicates
from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import (
    _EMPTY_PROJECTION_COLUMN,
    HiveTableMetadata,
    infer_table_metadata,
    read_hs2_batches,
)
from ray.data._internal.datasource_v2.formats.hive.hive_sql import (
    quote_identifier,
    split_predicate,
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
from ray.data._internal.datasource_v2.interfaces.pushdown import (
    SupportsColumnPruning,
    SupportsFilterPushdown,
    SupportsLimitPushdown,
)
from ray.data._internal.datasource_v2.interfaces.reader import Reader
from ray.data._internal.datasource_v2.interfaces.scanner import Scanner
from ray.data.expressions import Expr

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
    def __init__(self, spec: HiveReadSpec, schema: pa.Schema, **options):
        self._spec = spec
        self._schema = schema
        self._options = options

    def read(self, input_split: FileManifest) -> Iterator[pa.Table]:
        if len(input_split) != 1 or input_split.paths[0] != _READ_UNIT:
            raise ValueError("HiveServer2 requires exactly one read unit")
        yield from read_hs2_batches(self._spec, self._schema, **self._options)


@dataclass(frozen=True, eq=False)
class _HiveScanner(Scanner[FileManifest]):
    """Read caller-supplied SQL without advertising SQL rewrite capabilities."""

    _spec: HiveReadSpec
    _schema: pa.Schema

    def read_schema(self) -> pa.Schema:
        return self._schema

    def create_reader(self) -> _HiveReader:
        return _HiveReader(self._spec, self._schema)


@dataclass(frozen=True, eq=False)
class _HiveTableScanner(
    _HiveScanner, SupportsColumnPruning, SupportsFilterPushdown, SupportsLimitPushdown
):
    """Immutable table scan; accepted filters run before the SQL row limit."""

    native_types: Tuple[Tuple[str, str], ...]
    columns: Optional[Tuple[str, ...]] = None
    predicate: Optional[Expr] = field(default=None, repr=False)
    predicate_sql: Optional[str] = field(default=None, repr=False)
    limit: Optional[int] = None

    def read_schema(self) -> pa.Schema:
        if self.columns is None:
            return self._schema
        return pa.schema(
            [self._schema.field(name) for name in self.columns],
            metadata=self._schema.metadata,
        )

    def prune_columns(self, columns: List[str]) -> "_HiveTableScanner":
        available = self.read_schema().names
        if any(name not in available for name in columns):
            raise ValueError("HiveServer2 projection references an unavailable column")
        return replace(self, columns=tuple(columns))

    def pruned_column_names(self) -> Optional[Tuple[str, ...]]:
        return self.columns

    def push_filters(
        self, predicate: Expr
    ) -> Tuple["_HiveTableScanner", Optional[Expr]]:
        # Dataset filters offered after either limit must remain above that limit.
        if self._spec.limit is not None or self.limit is not None:
            return self, predicate
        accepted, sql, residual = split_predicate(
            predicate, self.read_schema(), self.native_types
        )
        if accepted is None:
            return self, residual
        combined_sql = (
            f"({self.predicate_sql} AND {sql})"
            if self.predicate_sql is not None
            else sql
        )
        return (
            replace(
                self,
                predicate=combine_predicates(self.predicate, accepted),
                predicate_sql=combined_sql,
            ),
            residual,
        )

    def pushed_predicate(self) -> Optional[Expr]:
        return self.predicate

    def push_limit(self, limit: int) -> "_HiveTableScanner":
        # Hive parses SQL LIMIT as a signed 32-bit integer. Keep larger
        # Dataset limits in Ray rather than changing their requested row count.
        if limit > 2**31 - 1:
            return self
        return replace(
            self, limit=limit if self.limit is None else min(limit, self.limit)
        )

    def pushed_limit(self) -> Optional[int]:
        limits = [
            limit for limit in (self._spec.limit, self.limit) if limit is not None
        ]
        return min(limits) if limits else None

    def create_reader(self) -> _HiveReader:
        identifier = self._spec.table_identifier
        assert identifier is not None
        database, table = identifier
        projection = (
            "*"
            if self.columns is None
            else ", ".join(quote_identifier(name) for name in self.columns)
        )
        if self.columns == ():
            projection = f"1 AS {quote_identifier(_EMPTY_PROJECTION_COLUMN)}"
        statement = f"SELECT {projection} FROM {quote_identifier(database)}.{quote_identifier(table)}"
        if self.predicate_sql is not None:
            statement += f" WHERE {self.predicate_sql}"
        limit = self.pushed_limit()
        if limit is not None:
            statement += f" LIMIT {limit}"
        return _HiveReader(
            self._spec,
            self.read_schema(),
            statement=statement,
            limit=limit,
            empty_projection=self.columns == (),
        )


class HiveDatasourceV2(DataSourceWithMetadata[FileManifest]):
    """Read a table or trusted query through one data operation per execution."""

    def __init__(self, spec: HiveReadSpec):
        super().__init__("Hive", DatasourceCategory.DATABASE)
        self._spec = spec
        self._table_metadata: Optional[HiveTableMetadata] = None

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
        self._table_metadata = infer_table_metadata(self._spec)
        return self._table_metadata.schema

    def create_scanner(self, schema: pa.Schema, filesystem=None, **options) -> Scanner:
        if self._spec.table is not None:
            if self._table_metadata is None:
                self.infer_schema(None)
            metadata = self._table_metadata
            assert metadata is not None
            if not schema.equals(metadata.schema, check_metadata=True):
                raise ValueError(
                    "HiveServer2 table metadata does not match the scan schema"
                )
            return _HiveTableScanner(self._spec, schema, metadata.native_types)
        return _HiveScanner(self._spec, schema)
