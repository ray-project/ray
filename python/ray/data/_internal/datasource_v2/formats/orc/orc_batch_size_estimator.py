"""Estimate ORC batch sizes from file-level metadata.

This optimization depends on the undocumented ``ORCFile.reader`` attribute and
its private ``pyarrow._orc.ORCReader.serialized_file_tail()`` method. If that
interface is unavailable, log at DEBUG and return no estimate so the reader
can use its default batch size.
"""

import logging
import math
from functools import lru_cache
from typing import FrozenSet, List, Optional, Protocol, Sequence, Type, cast

import pyarrow as pa
import pyarrow.dataset as pds
import pyarrow.orc as orc
from google.protobuf import descriptor_pb2, message_factory
from google.protobuf.message import DecodeError, Message

logger = logging.getLogger(__name__)


class _TypeFields(Protocol):
    subtypes: Sequence[int]
    field_names: Sequence[str]


class _FooterFields(Protocol):
    types: Sequence[Message]
    statistics: Sequence[Message]


class _FileTailFields(Protocol):
    footer: Message


@lru_cache(maxsize=1)
def _file_tail_message_type() -> Type[Message]:
    """Build the apache/orc-format 1.0.0 schema subset needed for batch sizing.

    These fields are wire-compatible with the apache/orc-format 1.1.1 schema.
    Using protobuf's dynamic messages preserves field presence without generated
    bindings or a separate ORC dependency. Unknown fields are not used for estimation.
    """
    field = descriptor_pb2.FieldDescriptorProto
    definition = descriptor_pb2.FileDescriptorProto(
        name="ray_data_orc_batch_sizing.proto", package="ray.data.orc", syntax="proto2"
    )
    # https://github.com/apache/orc-format/blob/v1.0.0/src/main/proto/orc/proto/orc_proto.proto
    messages = {
        "StringStatistics": [("sum", 3, field.TYPE_SINT64, False, "")],
        "BinaryStatistics": [("sum", 1, field.TYPE_SINT64, False, "")],
        "ColumnStatistics": [
            ("number_of_values", 1, field.TYPE_UINT64, False, ""),
            ("string_statistics", 4, field.TYPE_MESSAGE, False, "StringStatistics"),
            ("binary_statistics", 8, field.TYPE_MESSAGE, False, "BinaryStatistics"),
            ("has_null", 10, field.TYPE_BOOL, False, ""),
        ],
        "Type": [
            ("subtypes", 2, field.TYPE_UINT32, True, ""),
            ("field_names", 3, field.TYPE_STRING, True, ""),
        ],
        "Footer": [
            ("types", 4, field.TYPE_MESSAGE, True, "Type"),
            ("number_of_rows", 6, field.TYPE_UINT64, False, ""),
            ("statistics", 7, field.TYPE_MESSAGE, True, "ColumnStatistics"),
        ],
        "FileTail": [("footer", 2, field.TYPE_MESSAGE, False, "Footer")],
    }
    for name, fields in messages.items():
        message = definition.message_type.add(name=name)
        for name, number, field_type, repeated, type_name in fields:
            member = message.field.add(
                name=name,
                number=number,
                type=field_type,
                label=field.LABEL_REPEATED if repeated else field.LABEL_OPTIONAL,
            )
            if type_name:
                member.type_name = ".ray.data.orc." + type_name
    return message_factory.GetMessages([definition])["ray.data.orc.FileTail"]


def _optional_int(message: Message, name: str) -> Optional[int]:
    # The message schema is defined at runtime, so its fields have no stubs.
    return int(getattr(message, name)) if message.HasField(name) else None


def _array_size(
    data_type: pa.DataType,
    type_id: int,
    slots: int,
    types: Sequence[Message],
    statistics: Sequence[Message],
    collection_counts_valid: bool,
    ancestors: FrozenSet[int] = frozenset(),
) -> Optional[int]:
    """Estimate Arrow buffer bytes, including slots masked by parent nulls."""
    if type_id in ancestors or not 0 <= type_id < len(types):
        return None
    node, stats = cast(_TypeFields, types[type_id]), statistics[type_id]
    count = _optional_int(stats, "number_of_values")
    if count is None or count > slots:
        return None
    children = list(node.subtypes)
    names = list(node.field_names)
    validity = (slots + 7) // 8
    ancestors = ancestors | {type_id}

    if pa.types.is_struct(data_type):
        if names != [f.name for f in data_type] or len(children) != len(data_type):
            return None
        size = validity
        for child, child_id in zip(data_type, children):
            child_size = _array_size(
                child.type,
                child_id,
                slots,
                types,
                statistics,
                collection_counts_valid,
                ancestors,
            )
            if child_size is None:
                return None
            size += child_size
        return size

    if pa.types.is_list(data_type) or pa.types.is_map(data_type):
        if not collection_counts_valid:
            return None
        expected_children = 1 if pa.types.is_list(data_type) else 2
        if len(children) != expected_children or any(
            not 0 <= child < len(types) for child in children
        ):
            return None
        # ORC C++ 2.0.1 / 2.2.1 do not update collection lengths on the
        # no-null batch path. Do not trust total_children, even when present.
        # List elements must be explicitly non-null to infer all slots from
        # their value count. Map keys are non-null in valid Arrow maps.
        child_stats = statistics[children[0]]
        if _optional_int(child_stats, "has_null") != 0:
            return None
        child_slots = _optional_int(child_stats, "number_of_values")
        if child_slots is None:
            return None
        child_types = (
            [data_type.value_type]
            if pa.types.is_list(data_type)
            else [data_type.key_type, data_type.item_type]
        )
        size = validity + (slots + 1) * 4
        for child_type, child_id in zip(child_types, children):
            child_size = _array_size(
                child_type,
                child_id,
                child_slots,
                types,
                statistics,
                collection_counts_valid,
                ancestors,
            )
            if child_size is None:
                return None
            size += child_size
        return size

    if children:
        return None
    if pa.types.is_string(data_type) or pa.types.is_binary(data_type):
        name = (
            "string_statistics"
            if pa.types.is_string(data_type)
            else "binary_statistics"
        )
        if not stats.HasField(name):
            return None
        payload = _optional_int(getattr(stats, name), "sum")
        if payload is None or payload < 0 or (count == 0 and payload != 0):
            return None
        return validity + (slots + 1) * 4 + payload
    if (
        pa.types.is_boolean(data_type)
        or pa.types.is_integer(data_type)
        or pa.types.is_floating(data_type)
        or pa.types.is_decimal(data_type)
        or pa.types.is_date32(data_type)
        or pa.types.is_timestamp(data_type)
    ):
        return validity + (slots * data_type.bit_width + 7) // 8
    return None


def _estimate_projected_size(
    footer: Message,
    schema: pa.Schema,
    columns: Sequence[str],
    collection_counts_valid: bool = True,
) -> Optional[int]:
    rows = _optional_int(footer, "number_of_rows")
    fields = cast(_FooterFields, footer)
    types = list(fields.types)
    statistics = list(fields.statistics)
    if not rows or not types or len(types) != len(statistics):
        return None
    root = cast(_TypeFields, types[0])
    names = list(root.field_names)
    children = list(root.subtypes)
    if (
        names != schema.names
        or len(names) != len(children)
        or len(set(names)) != len(names)
    ):
        return None
    size = 0
    for name in columns:
        index = schema.get_field_index(name)
        if index < 0:
            return None
        field_size = _array_size(
            schema.field(index).type,
            children[index],
            rows,
            types,
            statistics,
            collection_counts_valid,
        )
        if field_size is None:
            return None
        size += field_size
    return size or None


def estimate_batch_size(
    fragment: pds.FileFragment,
    columns: Optional[List[str]],
    target_block_size: int,
    schema: Optional[pa.Schema] = None,
    excluded_columns: Sequence[str] = (),
) -> Optional[int]:
    """Estimate an initial batch row count from the first file's footer.

    The selected columns' estimated Arrow bytes give a file-wide average row
    width. This does not bound individual batch sizes or decoder memory usage.
    No data batches are read. Unsupported layouts, missing statistics and
    metadata failures return None so the reader can use its normal default.

    Args:
        fragment: Whole-file ORC fragment to inspect.
        columns: Output columns, or None for all physical columns.
        target_block_size: Target Arrow batch size in bytes.
        schema: Optional logical file schema used by the normal scanner.
        excluded_columns: Fields replaced by partition or synthesized values.

    Returns:
        Estimated row count, or None when no useful estimate is available.
    """
    if columns is not None and not any(
        name not in excluded_columns for name in columns
    ):
        return None
    try:
        with fragment.filesystem.open_input_file(fragment.path) as source:
            reader = orc.ORCFile(source)
            get_tail = getattr(
                getattr(reader, "reader", None), "serialized_file_tail", None
            )
            if not callable(get_tail):
                logger.debug("ORC batch sizing: serialized_file_tail is unavailable")
                return None
            if reader.writer_version == "ORIGINAL":
                return None
            physical_schema = reader.schema
            projected = [
                name
                for name in (physical_schema.names if columns is None else columns)
                if name not in excluded_columns
            ]
            if not projected:
                return None
            if schema is not None and any(
                schema.get_field_index(name) < 0
                or physical_schema.get_field_index(name) < 0
                or schema.field(name).type != physical_schema.field(name).type
                for name in projected
            ):
                return None
            data = get_tail()
            if not isinstance(data, bytes):
                return None
            tail = _file_tail_message_type().FromString(data)
            if not tail.HasField("footer"):
                return None
            footer = cast(_FileTailFields, tail).footer
            rows = _optional_int(footer, "number_of_rows")
            if rows != reader.nrows or not rows:
                return None
            # Collection value counts in the C++ writer are only updated
            # when indexes are enabled. Other selected layouts remain usable.
            size = _estimate_projected_size(
                footer,
                physical_schema,
                projected,
                collection_counts_valid=reader.row_index_stride > 0,
            )
            if size is None:
                return None
            return max(1, min(math.ceil(target_block_size / (size / rows)), rows))
    except (
        OSError,
        pa.ArrowInvalid,
        pa.ArrowNotImplementedError,
        DecodeError,
        ValueError,
    ) as e:
        logger.debug("Failed to estimate ORC batch size from metadata: %s", e)
        return None
