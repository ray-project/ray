"""The Iceberg per-file state that travels from listing to reading.

``FileManifest`` is the only payload the framework moves between ``ListFiles``
and ``ReadFiles``, and its three built-in columns (path, size, chunk metadata)
describe a plain file. An Iceberg read needs more: which delete files apply to
this data file, and which partition values it was written with. Those live in
Iceberg metadata that only the listing task (which called ``plan_files()``) has
seen, so they ride along as extra manifest columns -- ``FileManifest`` permits
any column beyond the three it requires.

Encode and decode are kept in one module because they are one contract: the
reader rebuilds a PyIceberg ``FileScanTask`` from these columns and hands it to
``ArrowScan``, which is the same object ``plan_files()`` produced. Only the
fields ``ArrowScan`` actually reads are carried -- the data file's path, format,
spec id, record count and partition record, and each delete file's path and
format -- not the per-column statistics and bounds a whole ``DataFile`` would
drag along.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Iterator, List, Optional, Sequence

import pyarrow as pa

from ray.data._internal.datasource_v2.listing.file_manifest import (
    FILE_CHUNK_METADATA_COLUMN_NAME,
    FILE_SIZE_COLUMN_NAME,
    PATH_COLUMN_NAME,
    FileManifest,
)

if TYPE_CHECKING:
    from pyiceberg.partitioning import PartitionSpec
    from pyiceberg.table import FileScanTask
    from pyiceberg.table.metadata import TableMetadata
    from pyiceberg.typedef import Record
    from pyiceberg.types import NestedField

# The data file's format name (``FileFormat`` enum value, e.g. ``"PARQUET"``).
FILE_FORMAT_COLUMN_NAME = "__ib_file_format"
# Partition spec the file was written under. A table can have several after
# partition evolution, and the values below are only interpretable against the
# right one.
SPEC_ID_COLUMN_NAME = "__ib_spec_id"
# The file's partition values, as an Arrow struct typed by the table's
# partition spec(s) -- the same shape Iceberg itself stores them in, inside its
# manifest files. See ``partition_arrow_type`` for the multi-spec layout.
PARTITION_COLUMN_NAME = "__ib_partition"
# Delete files that apply to this data file, as two parallel lists.
DELETE_PATHS_COLUMN_NAME = "__ib_delete_paths"
DELETE_FORMATS_COLUMN_NAME = "__ib_delete_formats"
# Rows the file holds, from Iceberg metadata. Nothing on the read path uses it;
# it is the input a metadata-only ``count()`` needs, and that runs in the read
# task (``SupportsMetadata.read_metadata`` is handed only the manifest), so it
# has to be a column or the "cheap" count would have to open every file. It is
# an *upper bound* whenever the file has delete files, so any consumer must
# guard on a clean file the way PyIceberg's own ``count()`` does.
RECORD_COUNT_COLUMN_NAME = "__ib_record_count"
# Whether Iceberg's own per-file residual is ``AlwaysTrue``, i.e. the file's
# partition values already satisfy the whole filter and no row of it can be
# filtered out. Nothing on the read path uses it -- ``ArrowScan`` re-applies the
# filter regardless -- but it is the test a metadata-only ``count()``, a
# native-Parquet fast path and an early-stopping limit all need, and it costs
# one bool to carry now instead of another listing change later.
RESIDUAL_IS_TRUE_COLUMN_NAME = "__ib_residual_is_true"


def _partition_fields(table_metadata: "TableMetadata") -> Dict[int, "NestedField"]:
    """Every partition field of every spec, keyed by partition-field id.

    A table can hold several partition specs after partition evolution, and a
    data file's partition record is positional against *its* spec. Iceberg
    allocates partition-field ids uniquely within a table (from 1000 up), so
    the union over specs keyed by that id is well defined: one id always means
    the same (source column, transform), hence the same type.
    """
    fields: Dict[int, "NestedField"] = {}
    schema = table_metadata.schema()
    for spec in table_metadata.partition_specs:
        for field in spec.partition_type(schema).fields:
            fields.setdefault(field.field_id, field)
    return fields


def _child_names(fields: Dict[int, "NestedField"]) -> Dict[int, str]:
    """Struct child name per partition-field id, computed identically on both sides.

    The field's own name, which is what makes a manifest readable, unless two
    specs gave different fields the same name -- then the id disambiguates.
    """
    collisions = {
        name
        for name in (field.name for field in fields.values())
        if sum(1 for f in fields.values() if f.name == name) > 1
    }
    return {
        field_id: (
            f"{field.name}__{field_id}" if field.name in collisions else field.name
        )
        for field_id, field in fields.items()
    }


def partition_arrow_type(table_metadata: "TableMetadata") -> Optional[pa.DataType]:
    """Arrow type of the partition column, or ``None`` for an unpartitioned table.

    A struct with one child per partition field of any spec, in field-id order,
    typed by the spec's own partition type: ``day(ts)`` is a ``date32``, not the
    source timestamp. A row fills only the children of the spec it was written
    under and leaves the rest null.
    """
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    fields = _partition_fields(table_metadata)
    if not fields:
        return None
    names = _child_names(fields)
    return pa.struct(
        [
            pa.field(
                names[field_id],
                schema_to_pyarrow(fields[field_id].field_type, include_field_ids=False),
            )
            for field_id in sorted(fields)
        ]
    )


def manifest_schema(table_metadata: "TableMetadata") -> pa.Schema:
    """Schema of the manifest block this module writes.

    Table-dependent, because the partition column is typed by the table's
    partition spec(s). The reader recomputes it from the same
    ``table_metadata`` the scanner carries, so both sides agree without the
    layout riding along in the block.
    """
    partition_type = partition_arrow_type(table_metadata)
    return pa.schema(
        [
            pa.field(PATH_COLUMN_NAME, pa.string()),
            pa.field(FILE_SIZE_COLUMN_NAME, pa.int64()),
            pa.field(FILE_CHUNK_METADATA_COLUMN_NAME, pa.null()),
            pa.field(FILE_FORMAT_COLUMN_NAME, pa.string()),
            pa.field(SPEC_ID_COLUMN_NAME, pa.int64()),
            pa.field(
                PARTITION_COLUMN_NAME,
                pa.null() if partition_type is None else partition_type,
            ),
            pa.field(DELETE_PATHS_COLUMN_NAME, pa.list_(pa.string())),
            pa.field(DELETE_FORMATS_COLUMN_NAME, pa.list_(pa.string())),
            pa.field(RECORD_COUNT_COLUMN_NAME, pa.int64()),
            pa.field(RESIDUAL_IS_TRUE_COLUMN_NAME, pa.bool_()),
        ]
    )


def _encode_partitions(
    tasks: Sequence["FileScanTask"], table_metadata: "TableMetadata"
) -> pa.Array:
    """The partition column: one struct per task, typed by the table's spec(s)."""
    partition_type = partition_arrow_type(table_metadata)
    if partition_type is None:
        return pa.nulls(len(tasks))

    fields = _partition_fields(table_metadata)
    names = _child_names(fields)
    specs = table_metadata.specs()
    columns: Dict[str, List[object]] = {
        name: [None] * len(tasks) for name in names.values()
    }
    for row, task in enumerate(tasks):
        record = task.file.partition
        # Positional: a partition record's i-th value belongs to the i-th field
        # of the spec it was written under, which is also the order
        # ``partition_type`` builds.
        for position, field in enumerate(specs[task.file.spec_id].fields):
            if position < len(record):
                columns[names[field.field_id]][row] = record[position]

    # Built child by child rather than from a list of dicts: pyarrow cannot
    # infer an extension-typed struct child from Python values (a UUID
    # partition column raises ``ArrowNotImplementedError``), but it accepts
    # Iceberg's internal representations directly per child -- an int for a
    # date, a ``Decimal`` for a decimal, 16 bytes for a UUID.
    children = [
        pa.array(columns[field.name], type=field.type) for field in partition_type
    ]
    return pa.StructArray.from_arrays(children, fields=list(partition_type))


def _decode_partitions(
    block: pa.Table, spec_ids: Sequence[int], table_metadata: "TableMetadata"
) -> List["Record"]:
    """Rebuild one PyIceberg partition ``Record`` per manifest row."""
    from pyiceberg.typedef import Record

    column = block[PARTITION_COLUMN_NAME]
    fields = _partition_fields(table_metadata)
    if not fields or pa.types.is_null(column.type):
        return [Record() for _ in spec_ids]

    from pyiceberg.partitioning import _to_partition_representation

    names = _child_names(fields)
    specs: Dict[int, "PartitionSpec"] = table_metadata.specs()
    # ``as_py`` gives the *logical* Python value (a ``datetime.date`` for a
    # date32 child); Iceberg's partition records hold the physical one (days
    # since epoch). ``_to_partition_representation`` is PyIceberg's own mapper
    # for exactly that, dispatched on the partition field's type.
    struct = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column
    child_values = {
        field.name: struct.field(index).to_pylist()
        for index, field in enumerate(struct.type)
    }
    records = []
    for row, spec_id in enumerate(spec_ids):
        values = []
        for field in specs[spec_id].fields:
            value = child_values[names[field.field_id]][row]
            values.append(
                None
                if value is None
                else _to_partition_representation(
                    fields[field.field_id].field_type, value
                )
            )
        records.append(Record(*values))
    return records


def manifest_from_scan_tasks(
    tasks: Sequence["FileScanTask"], table_metadata: "TableMetadata"
) -> FileManifest:
    """Encode PyIceberg scan tasks as one manifest block, one row per task."""
    from pyiceberg.expressions import AlwaysTrue

    paths: List[str] = []
    sizes: List[int] = []
    formats: List[str] = []
    spec_ids: List[int] = []
    delete_paths: List[List[str]] = []
    delete_formats: List[List[str]] = []
    record_counts: List[int] = []
    residual_is_true: List[bool] = []

    for task in tasks:
        data_file = task.file
        paths.append(data_file.file_path)
        sizes.append(data_file.file_size_in_bytes)
        formats.append(data_file.file_format.value)
        spec_ids.append(data_file.spec_id)
        record_counts.append(data_file.record_count)
        # Sorted so a manifest row is reproducible across executions; the set
        # PyIceberg hands us has no order.
        deletes = sorted(task.delete_files, key=lambda f: f.file_path)
        delete_paths.append([f.file_path for f in deletes])
        delete_formats.append([f.file_format.value for f in deletes])
        residual_is_true.append(isinstance(task.residual, AlwaysTrue))

    schema = manifest_schema(table_metadata)
    block = pa.table(
        [
            pa.array(paths, type=pa.string()),
            pa.array(sizes, type=pa.int64()),
            pa.nulls(len(paths)),
            pa.array(formats, type=pa.string()),
            pa.array(spec_ids, type=pa.int64()),
            _encode_partitions(tasks, table_metadata),
            pa.array(delete_paths, type=pa.list_(pa.string())),
            pa.array(delete_formats, type=pa.list_(pa.string())),
            pa.array(record_counts, type=pa.int64()),
            pa.array(residual_is_true, type=pa.bool_()),
        ],
        schema=schema,
    )
    return FileManifest(block)


def scan_tasks_from_manifest(
    manifest: FileManifest, table_metadata: "TableMetadata"
) -> Iterator["FileScanTask"]:
    """Rebuild one PyIceberg scan task per manifest row.

    The rebuilt ``DataFile``\\ s carry only the fields the read path reads; a
    delete file's ``record_count`` and ``file_size_in_bytes`` are 0 because
    nothing downstream of here consults them.
    """
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.table import FileScanTask

    block = manifest.as_block()
    columns = {
        name: block[name].to_pylist()
        for name in (
            PATH_COLUMN_NAME,
            FILE_SIZE_COLUMN_NAME,
            FILE_FORMAT_COLUMN_NAME,
            SPEC_ID_COLUMN_NAME,
            DELETE_PATHS_COLUMN_NAME,
            DELETE_FORMATS_COLUMN_NAME,
            RECORD_COUNT_COLUMN_NAME,
        )
    }
    spec_ids = columns[SPEC_ID_COLUMN_NAME]
    partitions = _decode_partitions(block, spec_ids, table_metadata)

    def build(
        content: DataFileContent,
        path: str,
        file_format: str,
        partition,
        spec_id: int,
        record_count: int,
        file_size: int,
    ) -> DataFile:
        data_file = DataFile.from_args(
            content=content,
            file_path=path,
            file_format=FileFormat(file_format),
            partition=partition,
            record_count=record_count,
            file_size_in_bytes=file_size,
        )
        # ``spec_id`` is not part of the on-disk record, so ``from_args``
        # can't set it.
        data_file.spec_id = spec_id
        return data_file

    for i in range(len(manifest)):
        spec_id = spec_ids[i]
        partition = partitions[i]
        data_file = build(
            DataFileContent.DATA,
            columns[PATH_COLUMN_NAME][i],
            columns[FILE_FORMAT_COLUMN_NAME][i],
            partition,
            spec_id,
            columns[RECORD_COUNT_COLUMN_NAME][i],
            columns[FILE_SIZE_COLUMN_NAME][i],
        )
        deletes = {
            build(
                DataFileContent.POSITION_DELETES,
                delete_path,
                delete_format,
                partition,
                spec_id,
                0,
                0,
            )
            for delete_path, delete_format in zip(
                columns[DELETE_PATHS_COLUMN_NAME][i],
                columns[DELETE_FORMATS_COLUMN_NAME][i],
            )
        }
        yield FileScanTask(data_file, delete_files=deletes)
