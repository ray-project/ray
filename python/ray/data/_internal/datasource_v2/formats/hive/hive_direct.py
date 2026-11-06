"""Hive metadata planning composed with native Parquet and ORC datasources."""

import logging
import re
from contextlib import suppress
from dataclasses import (
    dataclass,
    field as dataclass_field,
    fields as dataclass_fields,
    replace,
)
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.dataset as pds

from ray.data._internal.datasource_v2.common.file_reader import (
    _ARROW_DEFAULT_BATCH_SIZE,
    FileFormat,
)
from ray.data._internal.datasource_v2.common.listing_utils import sample_files
from ray.data._internal.datasource_v2.common.schema_inference import (
    _FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT,
    infer_schema_from_files,
)
from ray.data._internal.datasource_v2.formats.hive.hive_contract import HiveReadSpec
from ray.data._internal.datasource_v2.formats.hive.hive_hs2 import _connect
from ray.data._internal.datasource_v2.formats.hive.hive_schema import hive_type_to_arrow
from ray.data._internal.datasource_v2.formats.orc.orc_file_reader import OrcFileReader
from ray.data._internal.datasource_v2.formats.orc.orc_scanner import OrcScanner
from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
    ParquetFileReader,
)
from ray.data._internal.datasource_v2.formats.parquet.parquet_scanner import (
    ParquetScanner,
)
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
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns
from ray.data._internal.util import iterate_with_retry
from ray.data.context import DataContext

logger = logging.getLogger(__name__)

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
    force_positional_evolution: bool = False
    tolerate_missing_schema: bool = True
    schema_evolution_case_sensitive: bool = False


def _read_hive_setting(cursor, option: str, default=None):
    """Read one setting from the metadata cursor's actual HS2 session."""
    cursor.execute(f"SET {option}")
    rows = cursor.fetchall()
    if len(rows) != 1 or len(rows[0]) != 1:
        raise ValueError(f"Cannot determine Hive setting {option}")
    text = str(rows[0][0]).strip()
    if text == f"{option} is undefined":
        return default
    key, separator, value = text.partition("=")
    if key.strip() != option or not separator:
        raise ValueError(f"Cannot determine Hive setting {option}")
    return value.strip().lower()


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
        metadata = _parse_file_table(cursor.fetchall())
        if metadata.file_format == "parquet":
            value = _read_hive_setting(cursor, "parquet.column.index.access", "false")
            if value not in ("true", "false"):
                raise ValueError("Cannot determine Hive Parquet column mapping mode")
            if value == "true":
                raise ValueError(
                    "Hive direct-read does not support Parquet positional column access"
                )
        if metadata.file_format == "orc":
            # Impyla opens a separate HS2 session for each cursor. Query this
            # setting on the metadata cursor, including its factory overrides.
            value = _read_hive_setting(cursor, "orc.force.positional.evolution")
            if value not in ("true", "false"):
                raise ValueError("Cannot determine Hive ORC positional mapping mode")
            level = _read_hive_setting(
                cursor, "orc.force.positional.evolution.level", "1"
            )
            if level != "1":
                raise ValueError(
                    "Hive direct-read supports only top-level ORC positional evolution"
                )
            evolution = _read_hive_setting(cursor, "hive.exec.schema.evolution")
            if evolution != "true":
                raise ValueError("Hive direct-read requires ORC schema evolution")
            tolerate = _read_hive_setting(cursor, "orc.tolerate.missing.schema")
            if tolerate is None:
                tolerate = _read_hive_setting(
                    cursor, "hive.exec.orc.tolerate.missing.schema", "true"
                )
            if tolerate not in ("true", "false"):
                raise ValueError("Cannot determine Hive ORC missing schema mode")
            case_sensitive = _read_hive_setting(
                cursor, "orc.schema.evolution.case.sensitive", "true"
            )
            if case_sensitive not in ("true", "false"):
                raise ValueError("Cannot determine Hive ORC name matching mode")
            metadata = replace(
                metadata,
                force_positional_evolution=value == "true",
                tolerate_missing_schema=tolerate == "true",
                schema_evolution_case_sensitive=case_sensitive == "true",
            )
        return metadata
    finally:
        # A close failure must not mask the metadata lookup's original error.
        if cursor is not None:
            with suppress(Exception):
                cursor.close()
        with suppress(Exception):
            connection.close()


def _parse_file_table(rows) -> HiveFileTable:
    columns: List[pa.Field] = []
    details: Dict[str, str] = {}
    table_properties: Dict[str, str] = {}
    serde_properties: Dict[str, str] = {}
    in_columns = True
    properties: Optional[Dict[str, str]] = None
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
                    # DESCRIBE preserves literal column names, including spaces.
                    str(row[0]),
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
    file_format = _FORMATS.get(
        (details.get("InputFormat:", ""), details.get("SerDe Library:", ""))
    )
    positional_options = ("parquet.column.index.access",)
    if file_format != "orc":
        positional_options += ("orc.force.positional.evolution",)
    for option in positional_options:
        if any(
            properties.get(option, "false").lower() != "false"
            for properties in (table_properties, serde_properties)
        ):
            raise ValueError(
                "Hive direct-read does not support positional column access"
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
    if len({field.name.lower() for field in columns}) != len(columns):
        raise ValueError("Hive direct-read requires unambiguous column names")
    return HiveFileTable(location, file_format, pa.schema(columns))


def _compatible_read_type(expected: pa.DataType, actual: pa.DataType) -> pa.DataType:
    """Validate types, retaining file timestamp units until the typed Project."""
    if expected == actual or pa.types.is_null(actual):
        return expected
    if pa.types.is_signed_integer(expected) and pa.types.is_signed_integer(actual):
        if actual.bit_width < expected.bit_width:
            return expected
    if pa.types.is_float64(expected) and pa.types.is_float32(actual):
        return expected
    if pa.types.is_decimal128(expected) and pa.types.is_decimal128(actual):
        if actual.scale == expected.scale and actual.precision < expected.precision:
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


def _schema_fields_by_name(
    schema: pa.Schema,
    label: str,
    required_names: Optional[Set[str]] = None,
    *,
    normalize=str.casefold,
) -> Dict[str, pa.Field]:
    by_name: Dict[str, pa.Field] = {}
    for field in schema:
        folded = normalize(field.name)
        if folded in by_name:
            if required_names is None or folded in required_names:
                raise ValueError(f"Hive direct-read {label} has ambiguous column names")
            continue
        by_name[folded] = field
    return by_name


def _sampled_column_names(hive_schema: pa.Schema, file_schema: pa.Schema):
    by_name = _schema_fields_by_name(file_schema, "file schema", normalize=str.lower)
    return {
        field.name: by_name[field.name.lower()].name
        if field.name.lower() in by_name
        else field.name
        for field in hive_schema
    }


def _read_schema(hive_schema: pa.Schema, file_schema: pa.Schema):
    sampled_names = _sampled_column_names(hive_schema, file_schema)
    fields = []
    renames = {}
    for field in hive_schema:
        physical_name = sampled_names[field.name]
        physical_index = file_schema.get_field_index(physical_name)
        if physical_index < 0:
            fields.append(field)
            continue
        physical = file_schema.field(physical_index)
        read_type = _compatible_read_type(field.type, physical.type)
        fields.append(pa.field(physical.name, read_type))
        if physical.name != field.name:
            renames[physical.name] = field.name
    return pa.schema(fields), renames


def _validate_nested_field_names(
    expected: pa.DataType, actual: pa.DataType, path: str
) -> None:
    """Reject per-file nested name changes that would silently null-fill."""
    if pa.types.is_struct(expected) and pa.types.is_struct(actual):
        actual_by_name = _schema_fields_by_name(
            pa.schema(list(actual)), f"file schema at {path}"
        )
        for expected_field in expected:
            actual_index = actual.get_field_index(expected_field.name)
            if actual_index >= 0:
                _validate_nested_field_names(
                    expected_field.type,
                    actual[actual_index].type,
                    f"{path}.{expected_field.name}",
                )
                continue
            folded = actual_by_name.get(expected_field.name.casefold())
            if folded is not None:
                raise ValueError(
                    "Hive direct-read does not support per-file nested field "
                    f"name changes at {path}.{expected_field.name!s}: "
                    f"found {folded.name!r}"
                )
        return

    expected_is_list = (
        pa.types.is_list(expected)
        or pa.types.is_large_list(expected)
        or pa.types.is_fixed_size_list(expected)
    )
    actual_is_list = (
        pa.types.is_list(actual)
        or pa.types.is_large_list(actual)
        or pa.types.is_fixed_size_list(actual)
    )
    if expected_is_list and actual_is_list:
        _validate_nested_field_names(
            expected.value_type, actual.value_type, f"{path}[]"
        )
    elif pa.types.is_map(expected) and pa.types.is_map(actual):
        _validate_nested_field_names(
            expected.key_type, actual.key_type, f"{path}{{key}}"
        )
        _validate_nested_field_names(
            expected.item_type, actual.item_type, f"{path}{{value}}"
        )


def _validate_file_schema(
    hive_schema: pa.Schema,
    file_schema: pa.Schema,
    sampled_names: dict,
    required_fields: Optional[Set[str]] = None,
) -> None:
    """Validate the name mapping before the native reader can null-fill it."""
    required_names = (
        None
        if required_fields is None
        else {
            name.lower()
            for hive_name in required_fields
            for name in (hive_name, sampled_names[hive_name])
        }
    )
    actual_by_name = _schema_fields_by_name(
        file_schema, "file schema", required_names, normalize=str.lower
    )
    for hive_field in hive_schema:
        if required_fields is not None and hive_field.name not in required_fields:
            continue
        sampled_name = sampled_names[hive_field.name]
        file_index = file_schema.get_field_index(sampled_name)
        if file_index < 0:
            folded = actual_by_name.get(hive_field.name.lower())
            if folded is not None:
                raise ValueError(
                    "Hive direct-read does not support per-file column name "
                    f"changes for {hive_field.name!r}: expected "
                    f"{sampled_name!r}, found {folded.name!r}"
                )
            # A genuinely absent field retains the native reader's NULL fill.
            continue

        actual_field = file_schema.field(file_index)
        _validate_nested_field_names(
            hive_field.type, actual_field.type, hive_field.name
        )
        # A later file must satisfy the same finite compatibility rules as the
        # sample; native casting must not silently accept a narrowing instead.
        _compatible_read_type(hive_field.type, actual_field.type)


def _read_physical_schema(path: str, file_format: str, filesystem) -> pa.Schema:
    """Read only a file's schema for Hive mapping validation."""
    if file_format == "parquet":
        import pyarrow.parquet as pq

        with (
            pq.ParquetFile(path)
            if filesystem is None
            else pq.ParquetFile(path, filesystem=filesystem)
        ) as parquet_file:
            return parquet_file.schema_arrow

    import pyarrow.orc as orc

    if filesystem is None:
        return orc.ORCFile(path).schema
    with filesystem.open_input_file(path) as source:
        return orc.ORCFile(source).schema


def _read_physical_schema_with_retry(
    path: str, file_format: str, filesystem, retryable_errors: List[str]
) -> pa.Schema:
    """Read one file schema with the native reader's configured I/O retries."""
    return next(
        iterate_with_retry(
            lambda: [_read_physical_schema(path, file_format, filesystem)],
            f"read Hive {file_format} schema for {path}",
            match=retryable_errors,
        )
    )


def _required_hive_fields(reader, sampled_names: dict) -> Optional[Set[str]]:
    """Resolve projected and filter columns to their Hive top-level fields."""
    if reader._columns is None:
        return None

    required_columns = set(reader._columns)
    if reader._predicate is not None:
        from ray.data._internal.planner.plan_expression.expression_visitors import (
            get_column_references,
        )

        required_columns.update(get_column_references(reader._predicate))

    required_top_level = {name.split(".", maxsplit=1)[0] for name in required_columns}
    return {
        hive_name
        for hive_name, physical_name in sampled_names.items()
        if hive_name in required_top_level or physical_name in required_top_level
    }


class _HiveParquetSchemaValidatingReader(ParquetFileReader):
    """Validate per-file names before native Parquet reading and metadata."""

    def __init__(
        self,
        *,
        hive_schema: pa.Schema,
        sampled_names: Dict[str, str],
        **options,
    ):
        super().__init__(**options)
        self._hive_schema = hive_schema
        self._sampled_names = sampled_names

    def read(self, input_split: FileManifest):
        required_fields = _required_hive_fields(self, self._sampled_names)
        retryable_errors = DataContext.get_current().retried_io_errors
        for path in dict.fromkeys(str(path) for path in input_split.paths):
            file_schema = _read_physical_schema_with_retry(
                path,
                "parquet",
                self._filesystem,
                retryable_errors,
            )
            _validate_file_schema(
                self._hive_schema,
                file_schema,
                self._sampled_names,
                required_fields=required_fields,
            )
        yield from super().read(input_split)


@dataclass(frozen=True)
class _HiveParquetScanner(ParquetScanner):
    """Add Hive validation while retaining native Parquet readers and metadata."""

    hive_schema: pa.Schema = dataclass_field(kw_only=True, repr=False)
    sampled_names: Tuple[Tuple[str, str], ...] = dataclass_field(
        kw_only=True, repr=False
    )

    def create_reader(self) -> _HiveParquetSchemaValidatingReader:
        return _HiveParquetSchemaValidatingReader(
            hive_schema=self.hive_schema,
            sampled_names=dict(self.sampled_names),
            batch_size=self.batch_size,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            partitioning=self.partitioning,
            ignore_prefixes=self.ignore_prefixes,
            target_block_size=self.target_block_size,
            synthesized_columns=self.synthesized_columns,
            schema=self.schema,
            parquet_format_kwargs=dict(self.parquet_format_kwargs),
        )


def _orc_type_for_comparison(dtype):
    """Ignore only Arrow's ns/us representation of the same ORC TIMESTAMP."""
    if pa.types.is_timestamp(dtype) and dtype.unit == "ns":
        return pa.timestamp("us", tz=dtype.tz)
    if pa.types.is_struct(dtype):
        return pa.struct(
            [field.with_type(_orc_type_for_comparison(field.type)) for field in dtype]
        )
    if pa.types.is_list(dtype):
        return pa.list_(
            dtype.value_field.with_type(_orc_type_for_comparison(dtype.value_type))
        )
    if pa.types.is_map(dtype):
        return pa.map_(
            dtype.key_field.with_type(_orc_type_for_comparison(dtype.key_type)),
            dtype.item_field.with_type(_orc_type_for_comparison(dtype.item_type)),
            keys_sorted=dtype.keys_sorted,
        )
    return dtype


def _orc_uses_positions(hive_schema, file_schema, force, tolerate):
    if force:
        return True
    legacy = bool(file_schema.names) and all(
        re.fullmatch(r"_col[0-9]+", name) for name in file_schema.names
    )
    # The ORC adapter exposes TIMESTAMP as ns; Hive's output schema uses us.
    # Safe widening is not equality: it must still honor missing-schema policy.
    if legacy:
        expected, actual = (
            pa.schema(
                [
                    field.with_type(_orc_type_for_comparison(field.type))
                    for field in schema
                ]
            )
            for schema in (hive_schema, file_schema)
        )
        if not actual.equals(expected):
            if not tolerate:
                raise ValueError(
                    "Hive ORC file is missing schema names and tolerance is disabled"
                )
            return True
    return False


def _orc_column_indices(
    hive_schema, file_schema, force, tolerate, case_sensitive, required_fields=None
):
    if _orc_uses_positions(hive_schema, file_schema, force, tolerate):
        indices = [i if i < len(file_schema) else -1 for i in range(len(hive_schema))]
    else:
        names = file_schema.names
        if not case_sensitive:
            needed = (
                None
                if required_fields is None
                else {name.lower() for name in required_fields}
            )
            _schema_fields_by_name(
                file_schema, "file schema", needed, normalize=str.lower
            )
            names = [name.lower() for name in names]
        indices = []
        for field in hive_schema:
            name = field.name if case_sensitive else field.name.lower()
            if names.count(name) > 1 and (
                required_fields is None or field.name in required_fields
            ):
                raise ValueError(
                    "Hive direct-read file schema has ambiguous column names"
                )
            indices.append(names.index(name) if name in names else -1)
    for field, index in zip(hive_schema, indices):
        if index < 0 or (
            required_fields is not None and field.name not in required_fields
        ):
            continue
        actual = file_schema.field(index)
        _validate_nested_field_names(field.type, actual.type, field.name)
        _compatible_read_type(field.type, actual.type)
    return indices


def _infer_orc_read_schema(table, sample, filesystem):
    """Normalize each sampled file before native schema unification."""
    sampled_names = {field.name: field.name for field in table.schema}
    if not len(sample):
        return table.schema, sampled_names
    retryable = DataContext.get_current().retried_io_errors

    def read_schema(path: str) -> pa.Schema:
        schema = _read_physical_schema_with_retry(path, "orc", filesystem, retryable)
        indices = _orc_column_indices(
            table.schema,
            schema,
            table.force_positional_evolution,
            table.tolerate_missing_schema,
            table.schema_evolution_case_sensitive,
        )
        return pa.schema(
            [
                pa.field(
                    field.name,
                    _compatible_read_type(field.type, schema[index].type)
                    if index >= 0
                    else field.type,
                )
                for field, index in zip(table.schema, indices)
            ]
        )

    schema = infer_schema_from_files(
        sample.paths.tolist(), read_schema, format_name="ORC"
    )
    return schema, sampled_names


class _HiveOrcFileReader(OrcFileReader):
    """Adapt per-file ORC names with Arrow projection inside the native reader."""

    def __init__(
        self,
        *,
        hive_schema,
        sampled_names,
        force_positional_evolution,
        tolerate_missing_schema,
        schema_evolution_case_sensitive,
        **options,
    ):
        super().__init__(**options)
        self._hive_schema = hive_schema
        self._sampled_names = sampled_names
        self._force_positional_evolution = force_positional_evolution
        self._tolerate_missing_schema = tolerate_missing_schema
        self._schema_evolution_case_sensitive = schema_evolution_case_sensitive

    def _iter_fragment_tables(self, fragment, scanner_kwargs):
        """Project required Hive columns, then filter their logical values.

        Read only output and filter dependencies. Evaluate predicates after
        mapping so renamed and positional fields use Hive names and types.
        """
        required = _required_hive_fields(self, self._sampled_names)
        physical = fragment.physical_schema
        indices = _orc_column_indices(
            self._hive_schema,
            physical,
            self._force_positional_evolution,
            self._tolerate_missing_schema,
            self._schema_evolution_case_sensitive,
            required,
        )
        assert self._schema is not None
        fields = list(physical)
        projection = {}
        for field, index in zip(self._hive_schema, indices):
            if required is not None and field.name not in required:
                continue
            target = self._schema.field(field.name).type
            if index < 0:
                projection[field.name] = pds.scalar(pa.scalar(None, type=target))
            else:
                fields[index] = fields[index].with_type(target)
                projection[field.name] = pds.field(index)
        # A reader can scan fragments concurrently. Keep schema and kwargs local.
        kwargs: Dict[str, Any] = dict(scanner_kwargs)
        # The physical scan doesn't yet expose the Hive names used by predicates.
        kwargs.update(columns=projection, filter=None)
        scanner = fragment.scanner(schema=pa.schema(fields), **kwargs)
        predicate = (
            self._predicate.to_pyarrow() if self._predicate is not None else None
        )
        for tagged in scanner.scan_batches():
            table = pa.Table.from_batches([tagged.record_batch])
            raise_on_pickle_object_columns(table)
            if predicate is not None:
                table = table.filter(predicate)
            yield table


@dataclass(frozen=True)
class _HiveOrcScanner(OrcScanner):
    """Configure native ORC reads with per-file Hive column mapping."""

    hive_schema: pa.Schema = dataclass_field(kw_only=True, repr=False)
    sampled_names: Tuple[Tuple[str, str], ...] = dataclass_field(
        kw_only=True, repr=False
    )
    force_positional_evolution: bool = dataclass_field(kw_only=True, default=False)
    tolerate_missing_schema: bool = dataclass_field(kw_only=True, default=True)
    schema_evolution_case_sensitive: bool = dataclass_field(kw_only=True, default=False)

    def create_reader(self) -> _HiveOrcFileReader:
        return _HiveOrcFileReader(
            hive_schema=self.hive_schema,
            sampled_names=dict(self.sampled_names),
            force_positional_evolution=self.force_positional_evolution,
            tolerate_missing_schema=self.tolerate_missing_schema,
            schema_evolution_case_sensitive=self.schema_evolution_case_sensitive,
            format=FileFormat.ORC,
            batch_size=self.batch_size
            if self.batch_size is not None
            else _ARROW_DEFAULT_BATCH_SIZE,
            columns=list(self.columns) if self.columns is not None else None,
            predicate=self.predicate,
            limit=self.limit,
            filesystem=self.filesystem,
            ignore_prefixes=self.ignore_prefixes,
            schema=self.schema,
        )


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
        return pc.call_function(
            "if_else",
            [
                pc.call_function("less", [values, restored]),
                pc.call_function(
                    "subtract_checked",
                    [result, pa.scalar(1, pa.duration(target_type.unit))],
                ),
                result,
            ],
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
        offsets = pc.call_function("subtract", [offsets, offsets[0]])
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
        if merge_schema and len(sample) > _FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT:
            logger.warning(
                "Full Hive schema inference discovered %d candidate files, exceeding "
                "the %d-file warning threshold. Reading every file's schema on the "
                "driver before returning the Dataset may make planning slow.",
                len(sample),
                _FULL_SCHEMA_INFERENCE_WARNING_FILE_COUNT,
            )
        if table.file_format == "orc":
            self._schema, sampled_names = _infer_orc_read_schema(
                table, sample, source.filesystem
            )
            self.column_renames = {}
        else:
            physical = source.infer_schema(sample) if len(sample) else pa.schema([])
            sampled_names = _sampled_column_names(table.schema, physical)
            _validate_file_schema(table.schema, physical, sampled_names)
            self._schema, self.column_renames = _read_schema(table.schema, physical)
        self._hive_schema = table.schema
        self._sampled_names = sampled_names
        self._file_format = table.file_format
        self._force_positional_evolution = table.force_positional_evolution
        self._tolerate_missing_schema = table.tolerate_missing_schema
        self._schema_evolution_case_sensitive = table.schema_evolution_case_sensitive
        self._source: FileDataSourceV2 = source
        self._retained = sample if merge_schema else None

    def column_expressions(self):
        """Use existing typed projections for bounded strings and timestamp units.

        Keeping value changes in Project lets Ray retain filters on converted
        columns above the conversion, rather than pushing them into raw files.
        Timestamp values use Arrow kernels; container offsets and nulls are kept.
        """
        from ray.data.datatype import DataType
        from ray.data.expressions import Expr, col, pyarrow_udf

        expressions = {}
        for field, read_field in zip(self._hive_schema, self._schema):
            expression = col(field.name)
            if field.type != read_field.type:
                target = field.type

                def cast_time(values, target_type=target):
                    return _cast_hive_timestamps(values, target_type)

                cast_time_udf = cast(
                    Callable[[Expr], Expr],
                    pyarrow_udf(return_dtype=DataType.from_arrow(target))(cast_time),
                )
                expression = cast_time_udf(expression)
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
        scanner = cast(
            Union[ParquetScanner, OrcScanner],
            self._source.create_scanner(
                schema, filesystem=self._source.filesystem, **options
            ),
        )
        scanner_type = (
            _HiveParquetScanner if self._file_format == "parquet" else _HiveOrcScanner
        )
        scanner_options = {
            descriptor.name: getattr(scanner, descriptor.name)
            for descriptor in dataclass_fields(scanner)
            if descriptor.init
        }
        if self._file_format == "orc":
            scanner_options[
                "force_positional_evolution"
            ] = self._force_positional_evolution
            scanner_options["tolerate_missing_schema"] = self._tolerate_missing_schema
            scanner_options[
                "schema_evolution_case_sensitive"
            ] = self._schema_evolution_case_sensitive
        return scanner_type(
            **scanner_options,
            hive_schema=self._hive_schema,
            sampled_names=tuple(self._sampled_names.items()),
        )
