"""Parquet split-block bloom filter probing for row-group pruning.

PyArrow's dataset API never consults the bloom filters a writer may attach to
a column chunk, so a point lookup on an unsorted column decodes every row
group even when the file says the value cannot be there
(ray-project/ray#67569). This module closes that gap for equality predicates
(``==``, ``is_in``) during footer listing: it reads the bloom filter bytes of
the predicate's column chunk, replays the format's split-block bloom filter
(SBBF) check in pure Python, and reports "definitely not present" so
``FooterReader`` can drop the row group before a read task is ever built.

Everything here fails closed: no bloom filter, an unsupported physical or
logical type, a literal that cannot be serialized faithfully, a malformed
header, an IO error -- any doubt reads as "may match" and the row group is
kept. The only new IO is one small range read per (row group, predicate
column) that actually carries a bloom filter; files without them cost one
attribute read per column chunk and are pruned exactly as before.

The SBBF layout and the XXH64 keying follow parquet-format's BloomFilter.md;
the offsets come off the column chunk metadata, which pyarrow exposes as
``bloom_filter_offset`` / ``bloom_filter_length`` from 25.0.0 on
(apache/arrow#49927). On older pyarrow the attributes read as missing and
the whole check degrades to a no-op.
"""

from __future__ import annotations

import logging
import struct
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.fs import FileSystem
    from pyarrow.parquet import ColumnSchema, ParquetSchema, RowGroupMetaData


# ---------------------------------------------------------------------------
# XXH64: the hash Parquet bloom filters are keyed on (spec 0.1.1, seed 0).
# ---------------------------------------------------------------------------

_XXH_PRIME1 = 0x9E3779B185EBCA87
_XXH_PRIME2 = 0xC2B2AE3D27D4EB4F
_XXH_PRIME3 = 0x165667B19E3779F9
_XXH_PRIME4 = 0x85EBCA77C2B2AE63
_XXH_PRIME5 = 0x27D4EB2F165667C5
_U64_MASK = 0xFFFFFFFFFFFFFFFF


def _rotl64(x: int, r: int) -> int:
    return ((x << r) | (x >> (64 - r))) & _U64_MASK


def _xxh64_round(acc: int, lane: int) -> int:
    acc = (acc + lane * _XXH_PRIME2) & _U64_MASK
    acc = _rotl64(acc, 31)
    return (acc * _XXH_PRIME1) & _U64_MASK


def _xxh64_merge(acc: int, val: int) -> int:
    val = _xxh64_round(0, val)
    acc ^= val
    return (acc * _XXH_PRIME1 + _XXH_PRIME4) & _U64_MASK


def xxhash64(data: bytes, seed: int = 0) -> int:
    """XXH64 of ``data`` -- the hash SBBF membership is keyed on.

    Pure Python on purpose: a bloom filter probe hashes only the predicate's
    literals (one per ``==``, a handful per ``is_in``), never the data, so
    interpreter speed is plenty and Ray gains no dependency.
    """
    n = len(data)
    i = 0
    if n >= 32:
        v1 = (seed + _XXH_PRIME1 + _XXH_PRIME2) & _U64_MASK
        v2 = (seed + _XXH_PRIME2) & _U64_MASK
        v3 = seed & _U64_MASK
        v4 = (seed - _XXH_PRIME1) & _U64_MASK
        while i + 32 <= n:
            v1 = _xxh64_round(v1, int.from_bytes(data[i : i + 8], "little"))
            v2 = _xxh64_round(v2, int.from_bytes(data[i + 8 : i + 16], "little"))
            v3 = _xxh64_round(v3, int.from_bytes(data[i + 16 : i + 24], "little"))
            v4 = _xxh64_round(v4, int.from_bytes(data[i + 24 : i + 32], "little"))
            i += 32
        acc = (
            _rotl64(v1, 1) + _rotl64(v2, 7) + _rotl64(v3, 12) + _rotl64(v4, 18)
        ) & _U64_MASK
        acc = _xxh64_merge(acc, v1)
        acc = _xxh64_merge(acc, v2)
        acc = _xxh64_merge(acc, v3)
        acc = _xxh64_merge(acc, v4)
    else:
        acc = (seed + _XXH_PRIME5) & _U64_MASK

    acc = (acc + n) & _U64_MASK

    while i + 8 <= n:
        acc ^= _xxh64_round(0, int.from_bytes(data[i : i + 8], "little"))
        acc = (_rotl64(acc, 27) * _XXH_PRIME1 + _XXH_PRIME4) & _U64_MASK
        i += 8
    if i + 4 <= n:
        acc ^= (int.from_bytes(data[i : i + 4], "little") * _XXH_PRIME1) & _U64_MASK
        acc = (_rotl64(acc, 23) * _XXH_PRIME2 + _XXH_PRIME3) & _U64_MASK
        i += 4
    while i < n:
        acc ^= (data[i] * _XXH_PRIME5) & _U64_MASK
        acc = (_rotl64(acc, 11) * _XXH_PRIME1) & _U64_MASK
        i += 1

    acc ^= acc >> 33
    acc = (acc * _XXH_PRIME2) & _U64_MASK
    acc ^= acc >> 29
    acc = (acc * _XXH_PRIME3) & _U64_MASK
    acc ^= acc >> 32
    return acc


# ---------------------------------------------------------------------------
# Split-block bloom filter, per parquet-format's BloomFilter.md.
# ---------------------------------------------------------------------------

# Each block is 256 bits: eight 32-bit words, serialized little-endian. A
# value's hash picks one block by its high 32 bits, and one bit per word in
# that block by multiplying the low 32 bits with these salts and taking the
# top 5 bits of each 32-bit product.
_SALT = (
    0x47B6137B,
    0x44974D91,
    0x8824AD5B,
    0xA2B7289D,
    0x705495C7,
    0x2DF1424B,
    0x9EFC4947,
    0x5C6BFB31,
)
_BLOCK_BYTES = 32


def _sbbf_block_index(h: int, num_blocks: int) -> int:
    # The spec maps the hash's high 32 bits onto blocks with a multiply-shift
    # rather than a modulo: floor((h >> 32) * z / 2**32).
    return ((h >> 32) * num_blocks) >> 32


def _sbbf_check(bitset: bytes, h: int) -> bool:
    """Whether ``h`` may be inserted in ``bitset``; no false negatives."""
    num_blocks = len(bitset) // _BLOCK_BYTES
    if num_blocks == 0:
        # Not a valid SBBF; treat as "no opinion" so the caller keeps the group.
        return True
    offset = _sbbf_block_index(h, num_blocks) * _BLOCK_BYTES
    words = struct.unpack_from("<8I", bitset, offset)
    x = h & 0xFFFFFFFF
    for i in range(8):
        bit = ((x * _SALT[i]) & 0xFFFFFFFF) >> 27
        if not (words[i] >> bit) & 1:
            return False
    return True


def _sbbf_insert(bitset: bytearray, h: int) -> None:
    """Spec-conformant insertion; exists to round-trip-test the checker."""
    num_blocks = len(bitset) // _BLOCK_BYTES
    assert num_blocks > 0
    offset = _sbbf_block_index(h, num_blocks) * _BLOCK_BYTES
    words = list(struct.unpack_from("<8I", bitset, offset))
    x = h & 0xFFFFFFFF
    for i in range(8):
        bit = ((x * _SALT[i]) & 0xFFFFFFFF) >> 27
        words[i] |= 1 << bit
    struct.pack_into("<8I", bitset, offset, *words)


# ---------------------------------------------------------------------------
# BloomFilterHeader: thrift compact protocol, just enough to walk it.
# ---------------------------------------------------------------------------

_THRIFT_STOP = 0x00
# TCompactProtocol field-type ids.
_T_BOOL_TRUE = 0x01
_T_BOOL_FALSE = 0x02
_T_BYTE = 0x03
_T_I16 = 0x04
_T_I32 = 0x05
_T_I64 = 0x06
_T_DOUBLE = 0x07
_T_BINARY = 0x08
_T_LIST = 0x09
_T_SET = 0x0A
_T_MAP = 0x0B
_T_STRUCT = 0x0C
# Fixed-size types skip by byte count; varints and containers compute theirs.
_FIXED_SIZES = {_T_BYTE: 1, _T_DOUBLE: 8}
# Enough bytes for any header pyarrow or parquet-mr writes: the i32 plus
# three unions of an empty struct, with slack for future fields.
_HEADER_PROBE_BYTES = 64


def _read_varint(buf: bytes, pos: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def _skip_value(buf: bytes, pos: int, field_type: int) -> int:
    """Advance past one value of ``field_type``; returns the new position.

    A bool *field* carries its value in the field header itself and has no
    payload, while a bool *element* of a collection occupies a byte -- the
    collection loops below account for that themselves.
    """
    if field_type in (_T_BOOL_TRUE, _T_BOOL_FALSE):
        return pos
    if field_type in (_T_I16, _T_I32, _T_I64):
        _, pos = _read_varint(buf, pos)
        return pos
    if field_type in _FIXED_SIZES:
        return pos + _FIXED_SIZES[field_type]
    if field_type == _T_BINARY:
        size, pos = _read_varint(buf, pos)
        return pos + size
    if field_type in (_T_LIST, _T_SET):
        byte = buf[pos]
        pos += 1
        size = byte >> 4
        element_type = byte & 0x0F
        if size == 15:
            size, pos = _read_varint(buf, pos)
        for _ in range(size):
            if element_type in (_T_BOOL_TRUE, _T_BOOL_FALSE):
                pos += 1
            else:
                pos = _skip_value(buf, pos, element_type)
        return pos
    if field_type == _T_MAP:
        size, pos = _read_varint(buf, pos)
        if size:
            kv = buf[pos]
            pos += 1
            key_type, value_type = kv >> 4, kv & 0x0F
            for _ in range(size):
                pos = _skip_value(buf, pos, key_type)
                pos = _skip_value(buf, pos, value_type)
        return pos
    if field_type == _T_STRUCT:
        return _skip_struct(buf, pos)
    raise ValueError(f"unknown thrift compact type {field_type:#x}")


def _skip_struct(buf: bytes, pos: int) -> int:
    field_id = 0
    while True:
        byte = buf[pos]
        pos += 1
        if byte == _THRIFT_STOP:
            return pos
        field_id += byte >> 4
        pos = _skip_value(buf, pos, byte & 0x0F)


def _parse_union_member(buf: bytes, pos: int) -> Tuple[bool, int]:
    """Whether a header union's member is field id 1 (its only valid member).

    SplitBlockAlgorithm, XxHash and Uncompressed are all empty structs at
    field id 1 of their unions; a future member would land at a higher id and
    must disable the check rather than be guessed at.
    """
    member_is_first = False
    field_id = 0
    while True:
        byte = buf[pos]
        pos += 1
        if byte == _THRIFT_STOP:
            return member_is_first, pos
        field_id += byte >> 4
        if field_id == 1:
            member_is_first = True
        pos = _skip_value(buf, pos, byte & 0x0F)


def _parse_bloom_filter_header(data: bytes) -> Optional[Tuple[int, int]]:
    """Parse a ``BloomFilterHeader`` into ``(bitset_bytes, header_len)``.

    Returns ``None`` unless the header declares the one combination the
    checker implements: the split-block algorithm, xxHash, and no
    compression. Anything else -- or a truncated or malformed header --
    reads as "no usable bloom filter" and the caller keeps the row group.
    """
    try:
        pos = 0
        field_id = 0
        num_bytes: Optional[int] = None
        block_algorithm = xxhash_hash = uncompressed = False
        while True:
            byte = data[pos]
            pos += 1
            if byte == _THRIFT_STOP:
                break
            field_id += byte >> 4
            field_type = byte & 0x0F
            if field_id == 1 and field_type == _T_I32:
                # numBytes: the bitset size in bytes, as a zigzag varint.
                raw, pos = _read_varint(data, pos)
                num_bytes = (raw >> 1) ^ -(raw & 1)
            elif field_id in (2, 3, 4) and field_type == _T_STRUCT:
                # algorithm / hash / compression: unions whose only member
                # (field id 1) is an empty struct.
                member_is_first, pos = _parse_union_member(data, pos)
                if field_id == 2:
                    block_algorithm = member_is_first
                elif field_id == 3:
                    xxhash_hash = member_is_first
                else:
                    uncompressed = member_is_first
            else:
                pos = _skip_value(data, pos, field_type)
        if num_bytes is None or not (block_algorithm and xxhash_hash and uncompressed):
            return None
        # A bitset is whole 256-bit blocks.
        if num_bytes <= 0 or num_bytes % _BLOCK_BYTES != 0:
            return None
        return num_bytes, pos
    except (IndexError, ValueError, struct.error):
        return None


# ---------------------------------------------------------------------------
# Literals to the bytes the writer hashed.
# ---------------------------------------------------------------------------


def _serialize_literal(value: Any, column: "ColumnSchema") -> Optional[bytes]:
    """Serialize ``value`` to the bytes a bloom filter was keyed on, or ``None``.

    A bloom filter hashes each value's PLAIN encoding -- its *physical*
    Parquet bytes -- so a literal only translates when its Python type maps
    onto the column's physical type without any semantic conversion. Types
    whose comparison semantics involve a conversion return ``None`` and the
    caller keeps the row group:

    - decimals compare at a scale the literal must be shifted to, so the
      physical bytes are not the literal's own digits;
    - timestamps and times would need the literal's datetime spelled out in
      the column's unit;
    - INT96, FIXED_LEN_BYTE_ARRAY and BOOLEAN columns have no faithful
      fixed-width serialization here.

    Ints, floats and strings/binary -- the user_id / trace_id / order-number
    lookups bloom filters exist for -- all round-trip exactly.
    """
    if isinstance(value, bool):
        # A bool literal is not an int literal even though Python says so;
        # hashing it as 0/1 would answer the wrong question.
        return None
    try:
        logical = column.logical_type
        # pyarrow spells the logical type's kind in caps: "STRING", "DECIMAL",
        # "TIMESTAMP", ...; None for a plain physical column.
        logical_name = logical.type if logical is not None else None
        if logical_name == "DECIMAL":
            return None
        physical = column.physical_type
        if physical == "INT32":
            if isinstance(value, int) and -(2**31) <= value < 2**31:
                return struct.pack("<i", value)
            return None
        if physical == "INT64":
            if isinstance(value, int) and -(2**63) <= value < 2**63:
                return struct.pack("<q", value)
            return None
        if physical == "FLOAT":
            if isinstance(value, (int, float)):
                return struct.pack("<f", value)
            return None
        if physical == "DOUBLE":
            if isinstance(value, (int, float)):
                return struct.pack("<d", value)
            return None
        if physical == "BYTE_ARRAY":
            if isinstance(value, (bytes, bytearray)):
                return bytes(value)
            if isinstance(value, str) and logical_name == "STRING":
                return value.encode("utf-8")
            return None
        # INT96, FIXED_LEN_BYTE_ARRAY, BOOLEAN and anything else: no faithful
        # physical serialization, so no bloom filter opinion.
        return None
    except (struct.error, OverflowError, ValueError):
        # E.g. a double literal too large for a FLOAT column's bytes.
        return None


# ---------------------------------------------------------------------------
# IO and the footer-reader entry points.
# ---------------------------------------------------------------------------


def _read_bloom_filter(
    filesystem: "FileSystem",
    path: str,
    offset: int,
    length: Optional[int],
    retried_io_errors: Optional[Sequence[str]],
) -> Optional[bytes]:
    """Read one column chunk's bloom filter bitset, or ``None`` on any failure.

    Uses ``open_input_file`` + ``seek`` rather than ``open_input_stream``:
    the retrying filesystem wrapper Ray puts in front of remote filesystems
    does not forward the latter's ``start`` / ``length`` range arguments.

    ``bloom_filter_length`` covers the header plus the bitset, so the common
    path is a single read. Writers predating that field (or pyarrow's
    exposure of it) leave ``length`` unset; then the small header is read
    first to learn the bitset size, at the cost of a second range read.
    """
    from ray.data._internal.util import call_with_retry

    def _read(start: int, nbytes: int) -> bytes:
        with filesystem.open_input_file(path) as f:
            f.seek(start)
            return f.read(nbytes)

    try:
        if length is not None and length > 0:
            data = call_with_retry(
                lambda: _read(offset, length),
                description=f"read Parquet bloom filter for {path}",
                match=retried_io_errors,
            )
            header = _parse_bloom_filter_header(data)
            if header is None:
                return None
            num_bytes, header_len = header
            if header_len + num_bytes > len(data):
                return None
            return data[header_len : header_len + num_bytes]
        # No length on record: probe just enough bytes for the header, then
        # read the bitset it sizes.
        head = call_with_retry(
            lambda: _read(offset, _HEADER_PROBE_BYTES),
            description=f"read Parquet bloom filter header for {path}",
            match=retried_io_errors,
        )
        header = _parse_bloom_filter_header(head)
        if header is None:
            return None
        num_bytes, header_len = header
        bitset = call_with_retry(
            lambda: _read(offset + header_len, num_bytes),
            description=f"read Parquet bloom filter bitset for {path}",
            match=retried_io_errors,
        )
        return bitset if len(bitset) == num_bytes else None
    except Exception as e:
        logger.debug("Failed to read Parquet bloom filter for %s: %s", path, e)
        return None


def prepare_bloom_probes(
    schema: "ParquetSchema", predicates: Dict[str, Sequence[Any]]
) -> List[Tuple[int, List[int]]]:
    """Resolve predicate columns to ``(leaf index, hashes)`` probes for a file.

    A file has one schema, so this runs once per file and every row group
    reuses the result. A predicate column contributes a probe only when it
    names exactly one leaf -- the leaf's own dotted path, so flat columns and
    struct fields both work while list elements (``a.b.list.element``) do
    not -- and at least one literal serializes faithfully. Columns that
    contribute nothing simply leave their row groups alone.
    """
    probes: List[Tuple[int, List[int]]] = []
    if not predicates:
        return probes
    for leaf_index in range(len(schema)):
        column = schema.column(leaf_index)
        values = predicates.get(column.path)
        if values is None:
            continue
        hashes = []
        for value in values:
            payload = _serialize_literal(value, column)
            if payload is not None:
                hashes.append(xxhash64(payload))
        if hashes:
            probes.append((leaf_index, hashes))
    return probes


def row_group_may_match(
    *,
    filesystem: "FileSystem",
    path: str,
    row_group: "RowGroupMetaData",
    probes: Sequence[Tuple[int, Sequence[int]]],
    retried_io_errors: Optional[Sequence[str]] = None,
) -> bool:
    """Whether any probed value may occur in ``row_group``.

    ``False`` is authoritative: a bloom filter answered "definitely not" for
    every literal of some equality predicate on this row group, so the
    conjunction cannot hold and the group can be dropped. ``True`` means "no
    opinion" -- no bloom filter, an unreadable one, a false positive, or an
    error along the way -- and callers must treat it fail-closed.
    """
    try:
        for leaf_index, hashes in probes:
            chunk = row_group.column(leaf_index)
            # pyarrow >= 25 exposes these as attributes; on older versions
            # the getattr reads as missing, which simply disables the check.
            # An offset of 0 cannot be a real bloom filter (the file starts
            # with the "PAR1" magic), so it counts as absent too.
            offset = getattr(chunk, "bloom_filter_offset", None)
            if not offset:
                continue
            length = getattr(chunk, "bloom_filter_length", None)
            bitset = _read_bloom_filter(
                filesystem, path, offset, length, retried_io_errors
            )
            if bitset is None:
                continue
            if not any(_sbbf_check(bitset, h) for h in hashes):
                return False
        return True
    except Exception as e:
        logger.debug("Bloom filter check failed for %s: %s", path, e)
        return True
