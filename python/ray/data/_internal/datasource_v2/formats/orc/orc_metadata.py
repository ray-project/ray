"""Bounded, best-effort ORC tail statistics. Data I/O errors still propagate."""

import re
import zlib
from dataclasses import dataclass
from typing import Dict, Optional, Protocol, Tuple

import pyarrow as pa
from google.protobuf.message import DecodeError

from ray.data._internal.datasource_v2.formats.orc.orc_proto import (
    Footer,
    Metadata,
    PostScript,
)

# Bounds apply independently to Footer and Metadata. Files exceeding them
# simply use the existing streaming reader, including in default file mode.
_MAX_ENCODED = 16 * 1024 * 1024
_MAX_DECODED = 64 * 1024 * 1024
_MAX_BLOCK = 4 * 1024 * 1024


class MetadataUnavailable(ValueError):
    """The tail cannot be used for pruning; retain the original read unit."""


def _parse(message_type, data):
    message = message_type()
    try:
        message.ParseFromString(data)
    except (DecodeError, UnicodeDecodeError) as exc:
        raise MetadataUnavailable("Invalid ORC metadata protobuf") from exc
    return message


def _snappy_length(data: bytes) -> int:
    # The raw Snappy format starts with a uint32 varint uncompressed length.
    # Arrow requires this exact size, rather than an output-buffer capacity.
    value = 0
    for index, byte in enumerate(data[:5]):
        value |= (byte & 127) << (7 * index)
        if byte < 128:
            if value > 0xFFFFFFFF:
                break
            return value
    raise MetadataUnavailable("Invalid raw Snappy length")


def _decode_block(data: bytes, codec: int, capacity: int) -> bytes:
    try:
        if codec == 1:
            decoder = zlib.decompressobj(-15)
            result = decoder.decompress(data, capacity + 1)
            if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
                raise MetadataUnavailable("Invalid or oversized raw DEFLATE block")
        elif codec == 2:
            length = _snappy_length(data)
            if length > capacity:
                raise MetadataUnavailable("Oversized raw Snappy block")
            result = pa.Codec("snappy").decompress(data, length, asbytes=True)
        elif codec == 4:
            # Unlike size-prefixed or auto-detecting block APIs, specifying
            # uncompressed_size selects strictly raw LZ4 and returns actual bytes.
            import lz4.block

            try:
                result = lz4.block.decompress(data, uncompressed_size=capacity)
            except lz4.block.LZ4BlockError as exc:
                raise MetadataUnavailable("Invalid raw LZ4 block") from exc
        elif codec == 5:
            with pa.CompressedInputStream(pa.BufferReader(data), "zstd") as stream:
                result = stream.read(capacity + 1)
        else:
            raise MetadataUnavailable("Unsupported ORC compression")
    # Arrow's memory-only codec stream reports malformed frames as OSError.
    # File reads happen outside this scope and their OSErrors must propagate.
    except (
        ImportError,
        ValueError,
        RuntimeError,
        OSError,
        zlib.error,
        pa.ArrowException,
    ) as exc:
        raise MetadataUnavailable("ORC metadata codec unavailable or invalid") from exc
    if len(result) > capacity:
        raise MetadataUnavailable("Oversized ORC metadata block")
    return result


def _decode(data: bytes, codec: int, block_size: int) -> bytes:
    if codec == 0:
        if len(data) > _MAX_DECODED:
            raise MetadataUnavailable("Oversized uncompressed ORC metadata")
        return data
    if codec not in (1, 2, 4, 5) or not 0 < block_size <= _MAX_BLOCK:
        raise MetadataUnavailable("Unsupported codec or block size")
    output = bytearray()
    offset = 0
    while offset < len(data):
        if len(data) - offset < 3:
            raise MetadataUnavailable("Truncated ORC compression header")
        header = int.from_bytes(data[offset : offset + 3], "little")
        offset += 3
        length = header >> 1
        if not length or length > block_size or offset + length > len(data):
            raise MetadataUnavailable("Invalid ORC compression block length")
        block = data[offset : offset + length]
        offset += length
        if not header & 1:
            block = _decode_block(block, codec, block_size)
        if len(output) + len(block) > _MAX_DECODED:
            raise MetadataUnavailable("Oversized decoded ORC metadata")
        output.extend(block)
    return bytes(output)


def _read(source, offset: int, length: int) -> bytes:
    if length < 0 or length > _MAX_ENCODED or offset < 3:
        raise MetadataUnavailable("Invalid ORC tail range")
    data = source.read_at(length, offset)
    if len(data) != length:
        raise MetadataUnavailable("Truncated ORC tail")
    return data


@dataclass(frozen=True)
class ColumnStatistics:
    kind: int
    values: Optional[int] = None
    minimum: object = None
    maximum: object = None
    true_count: Optional[int] = None


class _PostScriptFields(Protocol):
    metadata_length: int
    compression: int
    compression_block_size: int

    def HasField(self, name: str) -> bool:
        ...


@dataclass(frozen=True)
class OrcMetadata:
    rows: int
    stripes: Tuple[Tuple[int, int, int], ...]
    columns: Dict[str, Tuple[int, int]]
    statistics: Dict[str, ColumnStatistics]
    postscript: _PostScriptFields
    metadata_offset: int
    string_statistics_safe: bool


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
    size = source.size()
    if size < 4:
        raise MetadataUnavailable("Not an ORC file")
    ps_length = _read(source, size - 1, 1)[0]
    ps_offset = size - 1 - ps_length
    ps = _parse(PostScript, _read(source, ps_offset, ps_length))
    if (
        not ps.HasField("magic")
        or ps.magic != "ORC"
        or tuple(ps.version) != (0, 12)
        or not ps.HasField("footer_length")
        or not ps.HasField("compression")
        or ps.stripe_statistics_length
    ):
        raise MetadataUnavailable("Unsupported ORC tail version/layout")
    footer_offset = ps_offset - ps.footer_length
    metadata_offset = footer_offset - ps.metadata_length
    if metadata_offset < 3:
        raise MetadataUnavailable("ORC tail overlaps header")
    footer = _parse(
        Footer,
        _decode(
            _read(source, footer_offset, ps.footer_length),
            ps.compression,
            ps.compression_block_size,
        ),
    )
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
    stripes = []
    previous_end = 3
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
        stripes.append((stripe.offset, end, stripe.number_of_rows))
        previous_end = end
    if sum(stripe[2] for stripe in stripes) != footer.number_of_rows:
        raise MetadataUnavailable("Inconsistent ORC row counts")
    # Java 2.2.1 compares complete UTF-8 bytes, including embedded NUL, and
    # distinguishes exact min/max from truncated bounds. C++ strncmp-based
    # string statistics do not satisfy this qualification.
    string_safe = footer.writer == 0 and version == (2, 2, 1) and ps.writer_version == 9
    return OrcMetadata(
        footer.number_of_rows,
        tuple(stripes),
        columns,
        _statistics(footer.statistics, footer.number_of_rows, columns, string_safe),
        ps,
        metadata_offset,
        string_safe,
    )


def read_stripe_statistics(source, metadata: OrcMetadata):
    ps = metadata.postscript
    if not ps.HasField("metadata_length") or not ps.metadata_length:
        raise MetadataUnavailable("Missing ORC stripe statistics")
    raw = _parse(
        Metadata,
        _decode(
            _read(source, metadata.metadata_offset, ps.metadata_length),
            ps.compression,
            ps.compression_block_size,
        ),
    )
    if len(raw.stripe_stats) != len(metadata.stripes):
        raise MetadataUnavailable("Incomplete ORC stripe statistics")
    return tuple(
        _statistics(
            stats.col_stats,
            stripe[2],
            metadata.columns,
            metadata.string_statistics_safe,
        )
        for stats, stripe in zip(raw.stripe_stats, metadata.stripes)
    )
