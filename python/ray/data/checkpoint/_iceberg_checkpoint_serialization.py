"""Serialize recoverable Iceberg write results for checkpoint recovery"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

import pyarrow as pa

from ray.data._internal.datasource.iceberg_datasink import IcebergWriteResult
from ray.data._internal.object_extensions.arrow import raise_on_pickle_object_columns

if TYPE_CHECKING:
    from pyiceberg.manifest import DataFile

_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("record_type", pa.string(), nullable=False),
        pa.field("content", pa.int32()),
        pa.field("file_path", pa.string()),
        pa.field("file_format", pa.string()),
        pa.field("partition_values", pa.list_(pa.binary())),
        pa.field("record_count", pa.int64()),
        pa.field("file_size_in_bytes", pa.int64()),
        pa.field("column_sizes", pa.map_(pa.int32(), pa.int64())),
        pa.field("value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("null_value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("nan_value_counts", pa.map_(pa.int32(), pa.int64())),
        pa.field("lower_bounds", pa.map_(pa.int32(), pa.binary())),
        pa.field("upper_bounds", pa.map_(pa.int32(), pa.binary())),
        pa.field("key_metadata", pa.binary()),
        pa.field("split_offsets", pa.list_(pa.int64())),
        pa.field("equality_ids", pa.list_(pa.int64())),
        pa.field("sort_order_id", pa.int32()),
        pa.field("spec_id", pa.int32()),
        pa.field("payload", pa.binary()),
    ]
)


def _enum_value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _map_value(value: Optional[Dict[int, Any]]) -> Optional[List[Tuple[int, Any]]]:
    return None if value is None else list(value.items())


def _serialize_arrow_table(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _deserialize_arrow_table(data: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        table = reader.read_all()
    raise_on_pickle_object_columns(table)
    return table


def _serialize_scalar(value: Any) -> bytes:
    return _serialize_arrow_table(pa.table({"value": [value]}))


def _deserialize_scalar(data: bytes) -> Any:
    table = _deserialize_arrow_table(data)
    if table.column_names != ["value"] or table.num_rows != 1:
        raise ValueError("Invalid Iceberg partition value in checkpoint payload")
    return table["value"][0].as_py()


def _serialize_schema(schema: pa.Schema) -> bytes:
    return _serialize_arrow_table(pa.Table.from_batches([], schema=schema))


def _deserialize_schema(data: bytes) -> pa.Schema:
    return _deserialize_arrow_table(data).schema


def _data_file_to_row(data_file: "DataFile") -> Dict[str, Any]:
    try:
        spec_id = data_file.spec_id
    except AttributeError:
        # PyIceberg 0.11's Arrow writer doesn't attach a spec ID to newly
        # produced DataFiles. Preserve that state rather than inventing one.
        spec_id = None
    return {
        "record_type": "data_file",
        "content": int(_enum_value(data_file.content)),
        "file_path": str(data_file.file_path),
        "file_format": str(_enum_value(data_file.file_format)),
        "partition_values": [_serialize_scalar(value) for value in data_file.partition],
        "record_count": data_file.record_count,
        "file_size_in_bytes": data_file.file_size_in_bytes,
        "column_sizes": _map_value(data_file.column_sizes),
        "value_counts": _map_value(data_file.value_counts),
        "null_value_counts": _map_value(data_file.null_value_counts),
        "nan_value_counts": _map_value(data_file.nan_value_counts),
        "lower_bounds": _map_value(data_file.lower_bounds),
        "upper_bounds": _map_value(data_file.upper_bounds),
        "key_metadata": data_file.key_metadata,
        "split_offsets": data_file.split_offsets,
        "equality_ids": data_file.equality_ids,
        "sort_order_id": data_file.sort_order_id,
        "spec_id": spec_id,
        "payload": None,
    }


def serialize_write_result(write_result: IcebergWriteResult) -> bytes:
    """Serialize Iceberg task output needed for append recovery."""
    if write_result.upsert_keys is not None:
        raise ValueError("Checkpointed Iceberg UPSERT is not supported")
    rows = [_data_file_to_row(data_file) for data_file in write_result.data_files]
    rows.extend(
        {"record_type": "schema", "payload": _serialize_schema(schema)}
        for schema in write_result.schemas
    )
    return _serialize_arrow_table(pa.Table.from_pylist(rows, schema=_PAYLOAD_SCHEMA))


def _pairs_to_dict(value: Any) -> Optional[Dict[int, Any]]:
    return dict(value) if value is not None else None


def _row_to_data_file(row: Dict[str, Any]) -> "DataFile":
    from pyiceberg.manifest import DataFile
    from pyiceberg.typedef import Record

    data_file = DataFile.from_args(
        content=row["content"],
        file_path=row["file_path"],
        file_format=row["file_format"],
        partition=Record(
            *[_deserialize_scalar(value) for value in row["partition_values"]]
        ),
        record_count=row["record_count"],
        file_size_in_bytes=row["file_size_in_bytes"],
        column_sizes=_pairs_to_dict(row["column_sizes"]),
        value_counts=_pairs_to_dict(row["value_counts"]),
        null_value_counts=_pairs_to_dict(row["null_value_counts"]),
        nan_value_counts=_pairs_to_dict(row["nan_value_counts"]),
        lower_bounds=_pairs_to_dict(row["lower_bounds"]),
        upper_bounds=_pairs_to_dict(row["upper_bounds"]),
        key_metadata=row["key_metadata"],
        split_offsets=row["split_offsets"],
        equality_ids=row["equality_ids"],
        sort_order_id=row["sort_order_id"],
    )
    if row["spec_id"] is not None:
        data_file.spec_id = row["spec_id"]
    return data_file


def deserialize_write_result(data: bytes) -> IcebergWriteResult:
    """Deserialize and validate a recoverable Iceberg task result."""
    table = _deserialize_arrow_table(data)
    if not table.schema.equals(_PAYLOAD_SCHEMA):
        raise ValueError("Unsupported Iceberg checkpoint Arrow schema")

    data_files = []
    schemas = []
    for row in table.to_pylist():
        if row["record_type"] == "data_file":
            data_files.append(_row_to_data_file(row))
        elif row["record_type"] == "schema" and row["payload"] is not None:
            schemas.append(_deserialize_schema(row["payload"]))
        else:
            raise ValueError("Invalid record in Iceberg checkpoint Arrow payload")
    return IcebergWriteResult(data_files=data_files, schemas=schemas)
