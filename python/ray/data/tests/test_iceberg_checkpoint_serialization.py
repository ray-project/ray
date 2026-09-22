import pickle
from datetime import date, datetime, timezone
from decimal import Decimal
from unittest.mock import patch

import pyarrow as pa
import pytest
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.typedef import Record

from ray.data._internal.datasource.iceberg_datasink import IcebergWriteResult
from ray.data.checkpoint import _iceberg_checkpoint_serialization as serialization
from ray.data.checkpoint._iceberg_checkpoint_serialization import (
    deserialize_write_result,
    serialize_write_result,
)


def _data_file(
    path: str,
    *,
    partition: Record,
    include_optional_metadata: bool = True,
    spec_id: int | None = 7,
) -> DataFile:
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=path,
        file_format=FileFormat.PARQUET,
        partition=partition,
        record_count=2,
        file_size_in_bytes=100,
        column_sizes={1: 20, 2: 40} if include_optional_metadata else None,
        value_counts={1: 2, 2: 2} if include_optional_metadata else None,
        null_value_counts={1: 0, 2: 1} if include_optional_metadata else None,
        nan_value_counts={1: 0, 2: 0} if include_optional_metadata else None,
        lower_bounds={1: b"a", 2: b"\x00"} if include_optional_metadata else None,
        upper_bounds={1: b"z", 2: b"\xff"} if include_optional_metadata else None,
        key_metadata=b"key" if include_optional_metadata else None,
        split_offsets=[4, 64] if include_optional_metadata else None,
        equality_ids=[1, 2] if include_optional_metadata else None,
        sort_order_id=3 if include_optional_metadata else None,
    )
    if spec_id is not None:
        data_file.spec_id = spec_id
    return data_file


def _write_result() -> IcebergWriteResult:
    partition = Record(
        "partition",
        date(2026, 9, 22),
        datetime(2026, 9, 22, 12, 30, tzinfo=timezone.utc),
        Decimal("12.34"),
        3,
        None,
    )
    return IcebergWriteResult(
        data_files=[
            _data_file("file:///warehouse/full.parquet", partition=partition),
            _data_file(
                "file:///warehouse/minimal.parquet",
                partition=Record(),
                include_optional_metadata=False,
                spec_id=None,
            ),
        ],
        schemas=[
            pa.schema(
                [pa.field("id", pa.int64()), pa.field("value", pa.string())],
                metadata={b"source": b"first"},
            ),
            pa.schema([pa.field("event_time", pa.timestamp("us", tz="UTC"))]),
        ],
    )


def _write_ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _read_ipc(data: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(data)) as reader:
        return reader.read_all()


def _assert_data_file_fields_equal(actual: DataFile, expected: DataFile) -> None:
    fields = (
        "content",
        "file_format",
        "record_count",
        "file_size_in_bytes",
        "column_sizes",
        "value_counts",
        "null_value_counts",
        "nan_value_counts",
        "lower_bounds",
        "upper_bounds",
        "key_metadata",
        "split_offsets",
        "equality_ids",
        "sort_order_id",
    )
    assert str(actual.file_path) == str(expected.file_path)
    assert list(actual.partition) == list(expected.partition)
    for field in fields:
        assert getattr(actual, field) == getattr(expected, field)
    assert getattr(actual, "spec_id", None) == getattr(expected, "spec_id", None)


def test_write_result_arrow_round_trip():
    result = _write_result()

    restored = deserialize_write_result(serialize_write_result(result))

    assert len(restored.data_files) == len(result.data_files)
    for actual, expected in zip(restored.data_files, result.data_files):
        _assert_data_file_fields_equal(actual, expected)
    assert restored.schemas == result.schemas
    assert restored.upsert_keys is None


def test_partition_is_serialized_once_per_data_file():
    result = _write_result()

    with patch.object(
        serialization,
        "_serialize_partition",
        wraps=serialization._serialize_partition,
    ) as serialize_partition:
        serialize_write_result(result)

    assert serialize_partition.call_count == len(result.data_files)


def test_write_result_rejects_upsert_keys():
    result = _write_result()
    result.upsert_keys = pa.table({"id": [1]})

    with pytest.raises(ValueError, match="UPSERT"):
        serialize_write_result(result)


def test_write_result_rejects_corrupt_and_pickle_payloads():
    with pytest.raises(ValueError, match="Invalid Arrow IPC"):
        deserialize_write_result(b"not an Arrow stream")
    with pytest.raises(ValueError, match="Invalid Arrow IPC"):
        deserialize_write_result(pickle.dumps({"data_files": []}))


def test_write_result_rejects_incompatible_schema_and_version():
    table = _read_ipc(serialize_write_result(_write_result()))
    incompatible_schema = table.append_column("unexpected", pa.nulls(len(table)))
    with pytest.raises(ValueError, match="schema or version"):
        deserialize_write_result(_write_ipc(incompatible_schema))

    incompatible_version = table.replace_schema_metadata(
        {serialization._FORMAT_VERSION_KEY: b"2"}
    )
    with pytest.raises(ValueError, match="schema or version"):
        deserialize_write_result(_write_ipc(incompatible_version))


def test_write_result_rejects_unknown_and_malformed_records():
    table = _read_ipc(serialize_write_result(_write_result()))
    record_types = table["record_type"].to_pylist()
    record_types[0] = "unknown"
    unknown_record = table.set_column(
        table.schema.get_field_index("record_type"),
        table.schema.field("record_type"),
        pa.array(record_types, type=pa.string()),
    )
    with pytest.raises(ValueError, match="Invalid record"):
        deserialize_write_result(_write_ipc(unknown_record))

    partitions = table["partition"].to_pylist()
    partitions[0] = b"invalid partition IPC"
    invalid_partition = table.set_column(
        table.schema.get_field_index("partition"),
        table.schema.field("partition"),
        pa.array(partitions, type=pa.binary()),
    )
    with pytest.raises(ValueError, match="Invalid data file"):
        deserialize_write_result(_write_ipc(invalid_partition))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
