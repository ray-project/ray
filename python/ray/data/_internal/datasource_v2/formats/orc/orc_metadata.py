"""Conservative file statistics from Arrow's serialized ORC footer."""

import re
from dataclasses import dataclass
from typing import Any, Dict, Optional, cast

from google.protobuf.message import DecodeError
from pyarrow import orc

from ray.data._internal.datasource_v2.formats.orc.orc_batch_size_estimator import (
    _file_tail_message_type,
)

_MAX_TAIL_BYTES = 64 * 1024 * 1024


class MetadataUnavailable(ValueError):
    """Statistics cannot safely establish that a file can be skipped."""


@dataclass(frozen=True)
class ColumnStatistics:
    kind: int
    values: Optional[int] = None
    minimum: object = None
    maximum: object = None
    true_count: Optional[int] = None


@dataclass(frozen=True)
class OrcMetadata:
    rows: int
    statistics: Dict[str, ColumnStatistics]


def _statistics(raw, rows, columns, string_safe):
    result = {}
    for name, (index, kind) in columns.items():
        if index >= len(raw):
            continue
        stat = raw[index]
        # Top-level, one-value-per-row scalars only. A present count and the
        # unit's reliable row count suffice; an independently present no-NULL
        # flag also proves exactly `rows` values. Missing fields are never zero.
        if kind in (10, 11, 12, 13):
            continue
        if stat.HasField("number_of_values"):
            count = stat.number_of_values
            if count > rows or (
                stat.HasField("has_null") and stat.has_null != (count < rows)
            ):
                continue
        elif stat.HasField("has_null") and not stat.has_null:
            count = rows
        else:
            continue
        low = high = true = None
        if kind in (1, 2, 3, 4) and stat.HasField("int_statistics"):
            bounds = stat.int_statistics
            if bounds.HasField("minimum") and bounds.HasField("maximum"):
                low, high = bounds.minimum, bounds.maximum
                bits = {1: 8, 2: 16, 3: 32, 4: 64}[kind]
                if not -(1 << (bits - 1)) <= low <= high < (1 << (bits - 1)):
                    low = high = None
        elif kind == 0 and stat.HasField("bucket_statistics"):
            counts = stat.bucket_statistics.count
            # Contradictory counts cannot prove all-NULL (or no-NULL),
            # even when the bucket has an unsupported shape.
            if any(value > count for value in counts):
                continue
            if len(counts) == 1:
                true = counts[0]
        elif kind == 7 and string_safe and stat.HasField("string_statistics"):
            bounds = stat.string_statistics
            if (
                bounds.HasField("minimum")
                and bounds.HasField("maximum")
                and not bounds.HasField("lower_bound")
                and not bounds.HasField("upper_bound")
            ):
                # Some proto2 runtimes return bytes for malformed UTF-8 rather
                # than raising DecodeError. Those bytes are not string bounds.
                if isinstance(bounds.minimum, str) and isinstance(bounds.maximum, str):
                    low, high = bounds.minimum, bounds.maximum
                    if low > high:
                        low = high = None
        result[name] = ColumnStatistics(kind, count, low, high, true)
    return result


def read_metadata(source) -> OrcMetadata:
    # Opening the Arrow reader can perform I/O; errors there must propagate.
    reader = orc.ORCFile(source)
    get_tail = getattr(getattr(reader, "reader", None), "serialized_file_tail", None)
    if not callable(get_tail):
        raise MetadataUnavailable("Serialized ORC footer is unavailable")
    try:
        data = get_tail()
    except NotImplementedError as exc:
        raise MetadataUnavailable("Serialized ORC footer is unsupported") from exc
    if not isinstance(data, bytes) or len(data) > _MAX_TAIL_BYTES:
        raise MetadataUnavailable("Invalid or oversized serialized ORC footer")
    try:
        tail = cast(Any, _file_tail_message_type().FromString(data))
    except (DecodeError, UnicodeDecodeError) as exc:
        raise MetadataUnavailable("Invalid ORC footer protobuf") from exc
    if not tail.HasField("footer") or not tail.HasField("postscript"):
        raise MetadataUnavailable("Incomplete serialized ORC tail")
    footer, ps = tail.footer, tail.postscript
    if (
        not ps.HasField("magic")
        or ps.magic != "ORC"
        or tuple(ps.version) != (0, 12)
        or not ps.HasField("footer_length")
        or ps.stripe_statistics_length
        or not tail.HasField("file_length")
        or not tail.HasField("postscript_length")
        or tail.file_length != source.size()
    ):
        raise MetadataUnavailable("Unsupported ORC tail version/layout")
    metadata_offset = (
        tail.file_length
        - 1
        - tail.postscript_length
        - ps.footer_length
        - ps.metadata_length
    )
    if metadata_offset < 3:
        raise MetadataUnavailable("ORC tail overlaps header")
    if (
        footer.HasField("encryption")
        or not footer.HasField("number_of_rows")
        or not footer.HasField("writer")
        or not ps.HasField("writer_version")
        or footer.writer not in (0, 1)
        or ps.writer_version < 6
        or not footer.HasField("software_version")
        or not isinstance(footer.software_version, str)
        or re.match(
            r"^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}(?:$|[-+])", footer.software_version
        )
        is None
    ):
        raise MetadataUnavailable("Unqualified ORC writer or encrypted footer")
    version = tuple(
        map(int, footer.software_version.split("-")[0].split("+")[0].split("."))
    )
    if not (1, 6, 0) <= version < (3, 0, 0) or (
        footer.writer == 1
        and ps.writer_version != 6
        or footer.writer == 0
        and ps.writer_version not in (6, 7, 8, 9)
    ):
        raise MetadataUnavailable("Unqualified ORC writer version")
    if (
        not footer.types
        or not footer.types[0].HasField("kind")
        or footer.types[0].kind != 12
    ):
        raise MetadataUnavailable("Expected ORC root struct")
    root = footer.types[0]
    if (
        len(root.subtypes) != len(root.field_names)
        or len(set(root.field_names)) != len(root.field_names)
        or not all(isinstance(name, str) for name in root.field_names)
    ):
        raise MetadataUnavailable("Invalid ORC root fields")
    columns = {}
    for name, index in zip(root.field_names, root.subtypes):
        if not 0 < index < len(footer.types) or not footer.types[index].HasField(
            "kind"
        ):
            raise MetadataUnavailable("Invalid ORC type id")
        columns[name] = (index, footer.types[index].kind)
    if len(set(root.subtypes)) != len(root.subtypes):
        raise MetadataUnavailable("Duplicate ORC type id")
    previous_end = 3
    stripe_rows = 0
    for stripe in footer.stripes:
        if not all(
            stripe.HasField(name)
            for name in (
                "offset",
                "index_length",
                "data_length",
                "footer_length",
                "number_of_rows",
            )
        ):
            raise MetadataUnavailable("Incomplete ORC stripe layout")
        end = (
            stripe.offset
            + stripe.index_length
            + stripe.data_length
            + stripe.footer_length
        )
        if stripe.offset < previous_end or end > metadata_offset:
            raise MetadataUnavailable("Overlapping ORC stripe ranges")
        previous_end = end
        stripe_rows += stripe.number_of_rows
    if (
        stripe_rows != footer.number_of_rows
        or reader.nrows != footer.number_of_rows
        or reader.nstripes != len(footer.stripes)
    ):
        raise MetadataUnavailable("Inconsistent ORC row or stripe counts")
    # Only this qualified Java writer is known to emit exact, ordered bounds.
    string_safe = footer.writer == 0 and version == (2, 2, 1) and ps.writer_version == 9
    return OrcMetadata(
        footer.number_of_rows,
        _statistics(footer.statistics, footer.number_of_rows, columns, string_safe),
    )
