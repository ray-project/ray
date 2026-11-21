"""Parquet test fixtures for schema inference."""

import struct

import pyarrow.parquet as pq

from ray.data._internal.datasource_v2.formats.parquet import (
    parquet_size_statistics as parquet_thrift,
)


def write_map_with_empty_struct(path, table):
    # PyArrow cannot write an empty struct. Add one to the generated footer
    # without changing the data pages, row groups, or column metadata.
    pq.write_table(table, path, store_schema=False)
    data = path.read_bytes()
    assert data[:4] == data[-4:] == b"PAR1"
    footer_size = struct.unpack("<I", data[-8:-4])[0]
    footer_start = len(data) - 8 - footer_size
    assert footer_start >= 4
    footer = bytearray(data[footer_start:-8])
    reader = parquet_thrift._CompactReader(data[footer_start:-8])
    schema_list_pos = root_children_pos = schema_end = None
    for field in reader._fields():
        if field.id != 2:
            reader._skip(field.type)
            continue
        assert field.type == parquet_thrift._LIST
        schema_list_pos = reader._pos
        header = reader._list_header()
        assert header.element_type == parquet_thrift._STRUCT and header.count == 5
        assert footer[schema_list_pos] == (header.count << 4) | parquet_thrift._STRUCT
        for root_field in reader._fields():
            if root_field.id == 5:
                assert root_field.type == parquet_thrift._I32
                root_children_pos = reader._pos
                assert reader._zigzag() == 1 and reader._pos == root_children_pos + 1
            else:
                reader._skip(root_field.type)
        for _ in range(header.count - 1):
            reader._skip_struct()
        schema_end = reader._pos
    assert reader._pos == len(footer)
    assert schema_list_pos is not None and root_children_pos is not None
    assert schema_end is not None
    footer[schema_list_pos] = (6 << 4) | parquet_thrift._STRUCT
    footer[root_children_pos] = 4  # zigzag(2)
    # SchemaElement: optional group named "empty", with zero children.
    empty_group = bytes((0x35, 0x02, 0x18, 0x05)) + b"empty" + bytes((0x15, 0, 0))
    footer[schema_end:schema_end] = empty_group
    path.write_bytes(
        data[:footer_start] + footer + struct.pack("<I", len(footer)) + b"PAR1"
    )
