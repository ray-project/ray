"""Hive metadata planning composed with native Parquet and ORC datasources."""

import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urlsplit

import pyarrow as pa

from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import _connect
from ray.data._internal.datasource_v2.formats.hive.hive_schema import hive_type_to_arrow
from ray.data._internal.datasource_v2.interfaces.datasource_v2 import (
    DatasourceCategory,
    DataSourceWithMetadata,
    FileDataSourceV2,
)
from ray.data._internal.datasource_v2.interfaces.file_indexer import (
    FileIndexer,
    FileInfo,
)
from ray.data._internal.datasource_v2.interfaces.file_manifest import FileManifest

_FORMATS = {
    (
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    ): "parquet",
    (
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    ): "orc",
}


@dataclass(frozen=True)
class HiveFileTable:
    """Validated nonpartitioned file table; contains no HS2 connection."""

    location: str
    file_format: str
    schema: pa.Schema


def read_file_table_metadata(spec: HiveReadSpec) -> HiveFileTable:
    """Read and validate Hive table metadata without executing a data query."""
    identifier = spec.table_identifier
    if identifier is None:
        raise ValueError("Hive direct-read supports table reads, not query reads")
    database, table = identifier
    connection = _connect(spec.connection_factory)
    cursor = None
    try:
        cursor = connection.cursor(user=spec.user)
        cursor.execute(f"DESCRIBE FORMATTED `{database}`.`{table}`")
        rows = cursor.fetchall()
    finally:
        # A close failure must not mask the metadata lookup's original error.
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()
    return _parse_file_table(rows)


def _parse_file_table(rows) -> HiveFileTable:
    columns = []
    details = {}
    table_properties = {}
    serde_properties = {}
    in_columns = True
    properties = None
    for row in rows:
        if len(row) < 3:
            raise ValueError("Hive direct-read requires complete table metadata")
        key, value, comment = (
            "" if item is None else str(item).strip() for item in row[:3]
        )
        if key.startswith("# Partition Information"):
            raise ValueError("Hive direct-read does not yet support partitioned tables")
        if key.startswith("# Detailed Table Information"):
            in_columns = False
        if key.startswith("#"):
            if in_columns and not (key == "# col_name" and value == "data_type"):
                raise ValueError("Unsupported Hive column metadata header")
            properties = None
            continue
        if not key and not value:
            continue
        if in_columns:
            if not key or not value:
                raise ValueError("Hive direct-read requires complete column metadata")
            columns.append(
                pa.field(
                    key,
                    hive_type_to_arrow(value),
                    metadata={b"hive_type": value.encode()},
                )
            )
        elif key == "Table Parameters:":
            properties = table_properties
        elif key == "Storage Desc Params:":
            properties = serde_properties
        elif key.endswith(":"):
            properties = None
            details[key] = value
        elif properties is not None:
            if key:
                properties[key.casefold()] = value
            else:
                properties[value.casefold()] = comment

    if details.get("Table Type:") not in ("MANAGED_TABLE", "EXTERNAL_TABLE"):
        raise ValueError("Hive direct-read requires a managed or external file table")
    if table_properties.get(
        "transactional", "false"
    ).lower() != "false" or table_properties.get("transactional_properties"):
        raise ValueError("Hive direct-read does not support Hive ACID tables")
    if table_properties.get("storage_handler") or table_properties.get(
        "table_type", ""
    ).lower() in ("iceberg", "delta", "hudi"):
        raise ValueError("Hive table formats require their dedicated reader")
    provider = table_properties.get("spark.sql.sources.provider", "hive").lower()
    if provider not in ("hive", "parquet", "orc"):
        raise ValueError("Hive direct-read does not support this table provider")
    for option in ("parquet.column.index.access", "orc.force.positional.evolution"):
        if any(
            properties.get(option, "false").lower() != "false"
            for properties in (table_properties, serde_properties)
        ):
            raise ValueError(
                "Hive direct-read does not support positional column access"
            )
    file_format = _FORMATS.get(
        (details.get("InputFormat:"), details.get("SerDe Library:"))
    )
    if file_format is None:
        raise ValueError("Hive direct-read requires a supported Parquet or ORC SerDe")
    location = details.get("Location:")
    if not location or not columns:
        raise ValueError("Hive direct-read requires a location and complete schema")
    uri = urlsplit(location)
    if uri.scheme not in ("", "file", "hdfs", "s3", "s3a"):
        raise ValueError("Hive direct-read requires a supported storage location")
    if "@" in uri.netloc:
        raise ValueError(
            "Configure storage credentials on the filesystem, not LOCATION"
        )
    if len({field.name.casefold() for field in columns}) != len(columns):
        raise ValueError("Hive direct-read requires unambiguous column names")
    return HiveFileTable(location, file_format, pa.schema(columns))


def _compatible_read_type(expected: pa.DataType, actual: pa.DataType) -> pa.DataType:
    """Validate types, retaining file timestamp units until the typed Project."""
    if expected == actual or pa.types.is_null(actual):
        return expected
    if pa.types.is_timestamp(expected) and pa.types.is_timestamp(actual):
        if expected.tz == actual.tz:
            return actual
    if pa.types.is_struct(expected) and pa.types.is_struct(actual):
        fields = []
        for field in expected:
            index = actual.get_field_index(field.name)
            if index >= 0:
                fields.append(
                    field.with_type(
                        _compatible_read_type(field.type, actual[index].type)
                    )
                )
            elif any(f.name.casefold() == field.name.casefold() for f in actual):
                raise ValueError(
                    "Hive nested field name case requires unsupported mapping"
                )
            else:
                fields.append(field)
        return pa.struct(fields)
    if pa.types.is_list(expected) and pa.types.is_list(actual):
        return pa.list_(_compatible_read_type(expected.value_type, actual.value_type))
    if pa.types.is_map(expected) and pa.types.is_map(actual):
        return pa.map_(
            _compatible_read_type(expected.key_type, actual.key_type),
            _compatible_read_type(expected.item_type, actual.item_type),
        )
    raise ValueError(f"Hive direct-read type mismatch: {expected} versus {actual}")


def _read_schema(hive_schema: pa.Schema, file_schema: pa.Schema):
    by_name = {}
    for field in file_schema:
        folded = field.name.casefold()
        if folded in by_name:
            raise ValueError("Hive direct-read file schema has ambiguous column names")
        by_name[folded] = field
    fields = []
    renames = {}
    for field in hive_schema:
        physical = by_name.get(field.name.casefold())
        if physical is None:
            fields.append(field)
            continue
        read_type = _compatible_read_type(field.type, physical.type)
        fields.append(pa.field(physical.name, read_type))
        if physical.name != field.name:
            renames[physical.name] = field.name
    return pa.schema(fields), renames


def _cast_hive_timestamps(values, target_type: pa.DataType):
    """Cast timestamp leaves to Hive microseconds, preserving Arrow containers."""
    import pyarrow.compute as pc

    from ray.data._internal.arrow_utils import _combine_as_list_array

    if values.type == target_type:
        return values
    if isinstance(values, pa.ChunkedArray):
        return pa.chunked_array(
            [_cast_hive_timestamps(chunk, target_type) for chunk in values.chunks],
            type=target_type,
        )
    if pa.types.is_timestamp(target_type):
        result = pc.cast(
            values, options=pc.CastOptions(target_type, allow_time_truncate=True)
        )
        # Arrow truncates epoch integers toward zero; Hive discards fractional
        # digits within the second. Correct only the negative fractional values.
        # Work in the target unit to avoid overflowing a nanosecond floor.
        restored = pc.cast(result, values.type)
        return pc.if_else(
            pc.less(values, restored),
            pc.subtract_checked(result, pa.scalar(1, pa.duration(target_type.unit))),
            result,
        )
    if pa.types.is_struct(target_type):
        return pa.StructArray.from_arrays(
            [
                _cast_hive_timestamps(values.field(i), field.type)
                for i, field in enumerate(target_type)
            ],
            fields=target_type,
            mask=values.is_null(),
        )
    # Native readers already aligned fields and types. Only existing list/map
    # containers containing timestamp leaves need their child arrays rebound.
    if pa.types.is_list(target_type) or pa.types.is_map(target_type):
        offsets = values.offsets
        start = offsets[0].as_py()
        length = offsets[-1].as_py() - start
        offsets = pc.subtract(offsets, offsets[0])
        if pa.types.is_list(target_type):
            return _combine_as_list_array(
                offsets=offsets,
                values=_cast_hive_timestamps(
                    values.values.slice(start, length), target_type.value_type
                ),
                null_mask=values.is_null(),
            )
        return pa.MapArray.from_arrays(
            offsets,
            _cast_hive_timestamps(
                values.keys.slice(start, length), target_type.key_type
            ),
            _cast_hive_timestamps(
                values.items.slice(start, length), target_type.item_type
            ),
            type=target_type,
            mask=values.is_null(),
        )
    raise TypeError("Hive timestamp conversion received an incompatible type")


class _HiveFileIndexer(FileIndexer):
    """Bind the native filesystem and, in full-schema mode, its retained list."""

    def __init__(self, source, retained: Optional[FileManifest]):
        self._indexer = source._get_file_indexer()
        self._paths = source.paths
        self._filesystem = source.filesystem
        self._retained = retained

    def list_files(self, paths, *, filesystem=None, **kwargs) -> Iterable[FileManifest]:
        if self._retained is not None:
            pruners = kwargs.pop("pruners", None) or []
            infos = (
                info
                for info in self.list_file_infos(paths)
                if all(pruner.should_include(info.path) for pruner in pruners)
            )
            yield from self._indexer.list_files_from_file_infos(
                infos, filesystem=self._filesystem, **kwargs
            )
        else:
            yield from self._indexer.list_files(
                pa.array(self._paths), filesystem=self._filesystem, **kwargs
            )

    def list_file_infos(
        self, paths, *, filesystem=None, **kwargs
    ) -> Iterable[FileInfo]:
        if self._retained is not None:
            for path, size in zip(self._retained.paths, self._retained.file_sizes):
                yield FileInfo(str(path), int(size))
        else:
            yield from self._indexer.list_file_infos(
                pa.array(self._paths), filesystem=self._filesystem, **kwargs
            )


class HiveDirectDatasource(DataSourceWithMetadata[FileManifest]):
    """Keep native indexing, grouping, scanning, and schema sampling for Hive."""

    def __init__(self, table: HiveFileTable, filesystem=None, merge_schema=False):
        super().__init__("HiveDirect", DatasourceCategory.DATABASE)
        if merge_schema and table.file_format != "parquet":
            raise ValueError("merge_schema is only supported for Parquet direct reads")
        # Hadoop table locations commonly use s3a; Arrow's native client uses s3.
        location = table.location
        if location.startswith("s3a://"):
            location = "s3://" + location[len("s3a://") :]
        if table.file_format == "parquet":
            from ray.data._internal.datasource_v2.formats.parquet.parquet_datasource_v2 import (
                ParquetDatasourceV2,
            )

            source = ParquetDatasourceV2(
                [location],
                filesystem=filesystem,
                partitioning=None,
                file_extensions=None,
            )
        else:
            from ray.data._internal.datasource_v2.formats.orc.orc_datasource_v2 import (
                OrcDatasourceV2,
            )

            source = OrcDatasourceV2(
                [location],
                filesystem=filesystem,
                partitioning=None,
                file_extensions=None,
            )
        if not source.paths:
            raise ValueError("Hive direct-read location could not be resolved")
        sample = sample_files(
            source._get_file_indexer(),
            source.paths,
            source.filesystem,
            max_files=None if merge_schema else 16,
        )
        physical = source.infer_schema(sample) if len(sample) else pa.schema([])
        self._schema, self.column_renames = _read_schema(table.schema, physical)
        self._hive_schema = table.schema
        self._source: FileDataSourceV2 = source
        self._retained = sample if merge_schema else None

    def column_expressions(self):
        """Use existing typed projections for bounded strings and timestamp units.

        Keeping value changes in Project lets Ray retain filters on converted
        columns above the conversion, rather than pushing them into raw files.
        Timestamp values use Arrow kernels; container offsets and nulls are kept.
        """
        from ray.data.datatype import DataType
        from ray.data.expressions import col, pyarrow_udf

        expressions = {}
        for field, read_field in zip(self._hive_schema, self._schema):
            expression = col(field.name)
            if field.type != read_field.type:
                target = field.type

                @pyarrow_udf(return_dtype=DataType.from_arrow(target))
                def cast_time(values, target_type=target):
                    return _cast_hive_timestamps(values, target_type)

                expression = cast_time(expression)
                expressions[field.name] = expression
            description = (field.metadata or {}).get(b"hive_type", b"").decode()
            bounded = re.fullmatch(
                r"(char|varchar)\s*\(\s*(\d+)\s*\)", description, re.IGNORECASE
            )
            if bounded:
                width = int(bounded[2])
                expression = expression.str.slice(0, width)
                if bounded[1].lower() == "char":
                    expression = expression.str.rpad(width)
                expressions[field.name] = expression
        return expressions

    @property
    def paths(self) -> List[str]:
        return ["hive://direct"]

    def _get_file_indexer(self) -> FileIndexer:
        return _HiveFileIndexer(self._source, self._retained)

    def get_file_partitioner(self, **kwargs):
        return self._source.get_file_partitioner(**kwargs)

    def infer_schema(self, sample: Optional[FileManifest]) -> pa.Schema:
        return self._schema

    def create_scanner(self, schema: pa.Schema, filesystem=None, **options):
        return self._source.create_scanner(schema, filesystem=self._source.filesystem)
