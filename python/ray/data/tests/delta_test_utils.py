"""Shared Delta Lake test helpers for the read and write tests."""

import json
import os
import struct
from typing import Any, Dict, Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

_Z85_ALPHABET = (
    "0123456789abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
)


def _z85(data: bytes) -> str:
    """Z85-encode ``data``, zero-padded to a multiple of 4 bytes."""
    data += b"\0" * (-len(data) % 4)
    chars = []
    for i in range(0, len(data), 4):
        value = int.from_bytes(data[i : i + 4], "big")
        block = []
        for _ in range(5):
            value, digit = divmod(value, 85)
            block.append(_Z85_ALPHABET[digit])
        chars.extend(reversed(block))
    return "".join(chars)


def _roaring_bitmap_array(positions: Iterable[int]) -> bytes:
    """Serialize ``positions`` as Delta's RoaringBitmapArray.

    The format: a magic number, the number of 32-bit bitmaps, then for each its
    high 32 bits and a portable 32-bit Roaring bitmap made of array containers.
    """
    by_high: Dict[int, List[int]] = {}
    for position in sorted(set(positions)):
        by_high.setdefault(position >> 32, []).append(position & 0xFFFFFFFF)
    out = struct.pack("<iq", 1681511377, len(by_high))
    for high, lows in sorted(by_high.items()):
        containers: Dict[int, List[int]] = {}
        for low in lows:
            containers.setdefault(low >> 16, []).append(low & 0xFFFF)
        header = struct.pack("<II", 12346, len(containers))
        for key, values in sorted(containers.items()):
            header += struct.pack("<HH", key, len(values) - 1)
        offsets, data = b"", b""
        first = len(header) + 4 * len(containers)
        for _, values in sorted(containers.items()):
            offsets += struct.pack("<I", first + len(data))
            data += b"".join(struct.pack("<H", value) for value in values)
        out += struct.pack("<i", high) + header + offsets + data
    return out


# The data files ``write_deletion_vector_table`` writes: two row groups of
# three rows, then one of three, and the rows their deletion vectors delete.
DELETION_VECTOR_FILES = {
    "part-0.parquet": ([0, 1, 2, 3, 4, 5], [1, 4]),
    "part-1.parquet": ([10, 11, 12], []),
}
# The rows ``read_delta`` returns from it.
DELETION_VECTOR_TABLE_ROWS = [0, 2, 3, 5, 10, 11, 12]


def write_deletion_vector_table(path: str) -> None:
    """Write a Delta table whose first file has a deletion vector.

    deltalake can't write deletion vectors, so the log is written by hand,
    with the deletion vector inline: Z85-encoded in the ``add`` action, as
    Databricks writes small ones. The files and deleted rows are
    ``DELETION_VECTOR_FILES``; the table column is ``x``.

    Args:
        path: Directory to create the table in. Must not exist yet.
    """
    os.makedirs(os.path.join(path, "_delta_log"))
    field = {"name": "x", "type": "long", "nullable": True, "metadata": {}}
    actions: List[Dict[str, Any]] = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": ["deletionVectors"],
                "writerFeatures": ["deletionVectors"],
            }
        },
        {
            "metaData": {
                "id": "3c3a3d8e-0000-4000-8000-000000000001",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": [field]}),
                "partitionColumns": [],
                "configuration": {"delta.enableDeletionVectors": "true"},
                "createdTime": 0,
            }
        },
    ]
    for name, (values, deleted) in DELETION_VECTOR_FILES.items():
        data_file = os.path.join(path, name)
        pq.write_table(
            pa.table({"x": pa.array(values, pa.int64())}), data_file, row_group_size=3
        )
        add: Dict[str, Any] = {
            "path": name,
            "partitionValues": {},
            "size": os.path.getsize(data_file),
            "modificationTime": 0,
            "dataChange": True,
            # A file with a deletion vector must record its row count.
            "stats": json.dumps(
                {
                    "numRecords": len(values),
                    "minValues": {"x": min(values)},
                    "maxValues": {"x": max(values)},
                    "nullCount": {"x": 0},
                }
            ),
        }
        if deleted:
            bitmap = _roaring_bitmap_array(deleted)
            add["deletionVector"] = {
                "storageType": "i",
                "pathOrInlineDv": _z85(bitmap),
                "sizeInBytes": len(bitmap),
                "cardinality": len(deleted),
            }
        actions.append({"add": add})
    with open(os.path.join(path, "_delta_log", f"{0:020d}.json"), "w") as f:
        f.write("\n".join(json.dumps(action) for action in actions) + "\n")


def write_column_mapped_table(path: str, mode: str) -> None:
    """Write a one-file reader 2 / writer 5 Delta table with column mapping.

    deltalake can't write a column-mapped table: it stores the property but
    keeps the column names in the data files and a reader 1 protocol. So the
    log is written by hand, with the data file storing column ``id`` (values 1
    and 2) under the physical name ``col-a1``, as Spark and Databricks write it.

    Args:
        path: Directory to create the table in. Must not exist yet.
        mode: The ``delta.columnMapping.mode`` to set, e.g. ``"name"`` or ``"id"``.
    """
    os.makedirs(os.path.join(path, "_delta_log"))
    data_file = os.path.join(path, "part-0.parquet")
    pq.write_table(pa.table({"col-a1": pa.array([1, 2], pa.int64())}), data_file)

    field = {
        "name": "id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "delta.columnMapping.id": 1,
            "delta.columnMapping.physicalName": "col-a1",
        },
    }
    actions = [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": "3c3a3d8e-0000-4000-8000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": [field]}),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": mode,
                    "delta.columnMapping.maxColumnId": "1",
                },
                "createdTime": 0,
            }
        },
        {
            "add": {
                "path": "part-0.parquet",
                "partitionValues": {},
                "size": os.path.getsize(data_file),
                "modificationTime": 0,
                "dataChange": True,
            }
        },
    ]
    with open(os.path.join(path, "_delta_log", f"{0:020d}.json"), "w") as f:
        f.write("\n".join(json.dumps(action) for action in actions) + "\n")


# The logical table ``write_mapped_table`` writes, as ``read_delta`` returns it.
MAPPED_TABLE_ROWS = [
    {"id": 1, "s": {"n": "x"}, "grp": "a", "extra": None},
    {"id": 2, "s": {"n": "y"}, "grp": "a", "extra": None},
    {"id": 10, "s": {"n": "z"}, "grp": "b", "extra": 5},
    {"id": 11, "s": None, "grp": "b", "extra": None},
]


def write_mapped_table(path: str, mode: str) -> None:
    """Write a column-mapped, partitioned Delta table as Databricks writes one.

    deltalake can't write column mapping, so the log is written by hand. The
    table has the columns ``id`` (long), ``s`` (struct with the field ``n``),
    ``grp`` (string, the partition column) and ``extra`` (long, added after the
    first file was written). Its two data files hold the rows of
    ``MAPPED_TABLE_ROWS``, one file per ``grp``, and, like Databricks's:

    - name every column by its physical name, nested fields too;
    - store the partition column as well;
    - carry a materialized row-tracking column that isn't a table column;
    - leave out ``extra`` in the first file, written before it was added.

    The log's statistics are keyed by physical name. In ``id`` mode the files
    name their columns differently from the log's physical names (as after a
    column rename), so only the Parquet field ids match them.

    Args:
        path: Directory to create the table in. Must not exist yet.
        mode: ``"name"`` or ``"id"``.
    """
    os.makedirs(os.path.join(path, "_delta_log"))
    physical = {
        "id": "col-id",
        "s": "col-s",
        "n": "col-n",
        "grp": "col-grp",
        "extra": "col-extra",
    }
    field_ids = {"id": 1, "s": 2, "n": 3, "grp": 4, "extra": 5}
    row_id_column = "_row-id-col-6f1"

    def file_name(logical: str) -> str:
        # In ``id`` mode the files' names differ from the log's physical names.
        return physical[logical] + ("-renamed" if mode == "id" else "")

    def file_field(logical: str, data_type: pa.DataType) -> pa.Field:
        return pa.field(
            file_name(logical),
            data_type,
            metadata={b"PARQUET:field_id": str(field_ids[logical]).encode()},
        )

    nested = pa.struct([file_field("n", pa.string())])
    add_actions = []
    for grp, rows in (("a", MAPPED_TABLE_ROWS[:2]), ("b", MAPPED_TABLE_ROWS[2:])):
        fields = [
            file_field("id", pa.int64()),
            file_field("s", nested),
            file_field("grp", pa.string()),
            pa.field(row_id_column, pa.int64()),
        ]
        columns: List[List[Any]] = [
            [row["id"] for row in rows],
            [
                None if row["s"] is None else {file_name("n"): row["s"]["n"]}
                for row in rows
            ],
            [grp] * len(rows),
            list(range(100, 100 + len(rows))),
        ]
        if grp == "b":
            fields.append(file_field("extra", pa.int64()))
            columns.append([row["extra"] for row in rows])
        relative = f"{physical['grp']}={grp}/part-0.parquet"
        data_file = os.path.join(path, relative)
        os.makedirs(os.path.dirname(data_file))
        pq.write_table(
            pa.Table.from_arrays(
                [pa.array(c, f.type) for c, f in zip(columns, fields)],
                schema=pa.schema(fields),
            ),
            data_file,
        )
        ids = [row["id"] for row in rows]
        add_actions.append(
            {
                "add": {
                    "path": relative,
                    "partitionValues": {physical["grp"]: grp},
                    "size": os.path.getsize(data_file),
                    "modificationTime": 0,
                    "dataChange": True,
                    "stats": json.dumps(
                        {
                            "numRecords": len(rows),
                            "minValues": {physical["id"]: min(ids)},
                            "maxValues": {physical["id"]: max(ids)},
                            "nullCount": {physical["id"]: 0},
                        }
                    ),
                }
            }
        )

    def log_field(logical: str, data_type: Any) -> Dict[str, Any]:
        return {
            "name": logical,
            "type": data_type,
            "nullable": True,
            "metadata": {
                "delta.columnMapping.id": field_ids[logical],
                "delta.columnMapping.physicalName": physical[logical],
            },
        }

    schema = {
        "type": "struct",
        "fields": [
            log_field("id", "long"),
            log_field("s", {"type": "struct", "fields": [log_field("n", "string")]}),
            log_field("grp", "string"),
            log_field("extra", "long"),
        ],
    }
    actions = [
        {"protocol": {"minReaderVersion": 2, "minWriterVersion": 5}},
        {
            "metaData": {
                "id": "3c3a3d8e-0000-4000-8000-000000000002",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(schema),
                "partitionColumns": ["grp"],
                "configuration": {
                    "delta.columnMapping.mode": mode,
                    "delta.columnMapping.maxColumnId": "5",
                    "delta.rowTracking.materializedRowIdColumnName": row_id_column,
                },
                "createdTime": 0,
            }
        },
        *add_actions,
    ]
    with open(os.path.join(path, "_delta_log", f"{0:020d}.json"), "w") as f:
        f.write("\n".join(json.dumps(action) for action in actions) + "\n")


# The columns ``write_type_widened_table`` widens: the type the first file
# stores, and the table type in Delta's and Arrow's names.
TYPE_WIDENINGS = {
    "i": (pa.int32(), "integer", "long", pa.int64()),
    "f": (pa.float32(), "float", "double", pa.float64()),
    "d": (pa.decimal128(5, 2), "decimal(5,2)", "decimal(10,4)", pa.decimal128(10, 4)),
    "day": (pa.date32(), "date", "timestamp_ntz", pa.timestamp("us")),
}


def write_type_widened_table(path: str, rows: Dict[str, List[Any]]) -> None:
    """Write a Delta table whose columns were widened after its first file.

    deltalake can't widen types, so the log is written by hand, as Databricks
    writes it: the ``typeWidening`` reader feature, and each column's
    ``delta.typeChanges``. The first file stores the narrow types of
    ``TYPE_WIDENINGS``, the second the wide ones.

    Args:
        path: Directory to create the table in. Must not exist yet.
        rows: Each column's values, half in each file.
    """
    os.makedirs(os.path.join(path, "_delta_log"))
    half = len(next(iter(rows.values()))) // 2
    add_actions = []
    for index, (start, end, wide) in enumerate(((0, half, False), (half, None, True))):
        name = f"part-{index}.parquet"
        data_file = os.path.join(path, name)
        pq.write_table(
            pa.table(
                {
                    column: pa.array(
                        values[start:end],
                        TYPE_WIDENINGS[column][3 if wide else 0],
                    )
                    for column, values in rows.items()
                }
            ),
            data_file,
        )
        add_actions.append(
            {
                "add": {
                    "path": name,
                    "partitionValues": {},
                    "size": os.path.getsize(data_file),
                    "modificationTime": 0,
                    "dataChange": True,
                }
            }
        )
    fields = [
        {
            "name": column,
            "type": TYPE_WIDENINGS[column][2],
            "nullable": True,
            "metadata": {
                "delta.typeChanges": [
                    {
                        "fromType": TYPE_WIDENINGS[column][1],
                        "toType": TYPE_WIDENINGS[column][2],
                    }
                ]
            },
        }
        for column in rows
    ]
    features = ["typeWidening", "timestampNtz"]
    actions = [
        {
            "protocol": {
                "minReaderVersion": 3,
                "minWriterVersion": 7,
                "readerFeatures": features,
                "writerFeatures": features,
            }
        },
        {
            "metaData": {
                "id": "3c3a3d8e-0000-4000-8000-000000000004",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps({"type": "struct", "fields": fields}),
                "partitionColumns": [],
                "configuration": {"delta.enableTypeWidening": "true"},
                "createdTime": 0,
            }
        },
        *add_actions,
    ]
    with open(os.path.join(path, "_delta_log", f"{0:020d}.json"), "w") as f:
        f.write("\n".join(json.dumps(action) for action in actions) + "\n")
