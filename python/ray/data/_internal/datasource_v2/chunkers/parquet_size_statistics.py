"""Recover Parquet ``SizeStatistics`` from the footer bytes PyArrow already holds.

``SizeStatistics`` (parquet-format 2.10) carries the one quantity needed to size
a decoded Arrow block that nothing else in the footer provides: the total
character bytes of ``BYTE_ARRAY`` leaves. PyArrow's C++ writer emits the struct
by default, and arrow-rs exposes it as
``ColumnChunkMetaData::unencoded_byte_array_data_bytes``, but PyArrow's Python
bindings declare no getter for it.

The data is still reachable with zero extra I/O: ``FileMetaData.__reduce__()``
hands back the raw Thrift-serialized footer, so a targeted TCompactProtocol walk
can read it out of the footer ``ListFiles`` already fetched.

The walk is deliberately narrow -- ``FileMetaData.4 -> RowGroup.1 ->
ColumnChunk.3 -> ColumnMetaData.16`` -- and skips every other field generically.
That is both far cheaper than a full parse (which would materialize every
``SchemaElement`` and every ``Statistics`` min/max blob) and keeps the surface
small enough to delete outright if PyArrow ever binds the accessor.

The same approach is what arrow-rs adopted to make footer parsing 3-9x faster
than its generated Thrift parser, and the reasoning transfers directly:

    https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/

Two of its lessons shape the code below. First, skipped bytes must be *scanned*
but need not be *decoded* -- Thrift's compact encoding is variable-length, so
there is no random access, but stepping over a field is much cheaper than
building a value from it. Second, the cost of a targeted walk is dominated by
per-field overhead, which in a generated Rust parser means small heap
allocations and here means Python-level calls and tuples. So the skip path
decodes nothing it does not need for width, materializes no ``(id, type)``
pairs, and reads the buffer through local variables rather than a method call
per byte.

On correctness: hand-decoded binary fails by returning wrong numbers, not by
raising, because a one-byte cursor desync leaves the field-id deltas decoding
onto plausible but wrong ids. Nothing downstream can catch that -- a decoded size
has no independent source to be checked against -- so the guards are all up
front: type assertions on every field this walk claims to recognize, a shape
check on the row-group and leaf counts, and a differential test against
``thriftpy2``'s own compact protocol over a matrix of schemas. Treat that
differential test as the module's real contract; changes to the skip paths in
particular should not be trusted without it.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator, List, NamedTuple, Optional, Tuple

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    # Behind TYPE_CHECKING so this module stays importable without PyArrow, which
    # keeps the protocol reader testable in isolation.
    from pyarrow.parquet import FileMetaData

# TCompactProtocol type ids.
_BOOL_TRUE, _BOOL_FALSE, _BYTE, _I16, _I32, _I64 = 1, 2, 3, 4, 5, 6
_DOUBLE, _BINARY, _LIST, _SET, _MAP, _STRUCT = 7, 8, 9, 10, 11, 12

# Field ids from the parquet-format Thrift schema, the authoritative source for
# every constant below:
# https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
# (see `struct FileMetaData`, `struct RowGroup`, `struct ColumnChunk`,
# `struct ColumnMetaData` and `struct SizeStatistics`; line numbers drift, the
# struct names do not).
#
# These ids are the wire identity of a field -- the serialized bytes carry the id,
# not the name -- so parquet-format can only ever append, never renumber. Fields
# 14/15 were added in 2.10 and geospatial took 17 rather than displacing 16; a
# future field 18 is handled by the generic skip.
_FILE_META_ROW_GROUPS = 4
_ROW_GROUP_COLUMNS = 1
_COLUMN_CHUNK_META_DATA = 3
_COLUMN_META_SIZE_STATISTICS = 16
_SIZE_STATS_UNENCODED_BYTES = 1
_SIZE_STATS_REPETITION_HISTOGRAM = 2
_SIZE_STATS_DEFINITION_HISTOGRAM = 3


class LeafSizeStats(NamedTuple):
    """One leaf column chunk's ``SizeStatistics``.

    ``unencoded_byte_array_data_bytes`` is ``None`` for every non-``BYTE_ARRAY``
    leaf: the spec restricts the field to that physical type, and fixed-width
    leaves get their width from the schema instead. Both histograms may be empty,
    which the spec permits when ``max_repetition_level`` is 0 or
    ``max_definition_level`` is at most 1.
    """

    unencoded_byte_array_data_bytes: Optional[int]
    repetition_level_histogram: Tuple[int, ...]
    definition_level_histogram: Tuple[int, ...]


class _ThriftDesync(Exception):
    """A decoded field's type contradicts parquet.thrift.

    Raised instead of skipping, because a type mismatch on a field we claim to
    recognize means the cursor has drifted and the *ids* are no longer
    trustworthy, not that the file is using a newer schema.
    """


class _CompactReader:
    """A minimal TCompactProtocol reader over a footer buffer.

    Only the traversal path is decoded; everything else goes through
    :meth:`_skip`. Unknown field ids and types are tolerated so a newer
    parquet-format writer cannot break the walk.
    """

    __slots__ = ("_buf", "_pos")

    def __init__(self, buf):
        self._buf = buf
        self._pos = 0

    def _byte(self) -> int:
        # Indexing past the end raises IndexError, which read_size_statistics
        # turns into a clean fallback -- a truncated footer must never produce a
        # partial answer.
        value = self._buf[self._pos]
        self._pos += 1
        return value

    def _varint(self) -> int:
        """Decode an unsigned base-128 varint.

        Each byte carries 7 payload bits in its low bits, with the high bit set to
        mean "another byte follows". Payload groups are ordered least-significant
        first, so byte *i* contributes its 7 bits at position ``7 * i`` -- hence
        the running ``shift``. Small values therefore cost one byte, which is the
        whole point: most Parquet footer integers are small.

        Example: ``0x80 0xFA 0x01`` -> low group ``0x00``, then ``0x7A << 7``,
        then ``0x01 << 14`` == 32000, which :meth:`_zigzag` in turn reads as
        16000.

        Deliberately does not go through :meth:`_byte`: a footer runs to hundreds
        of thousands of bytes, and hoisting the buffer and cursor into locals for
        the duration turns a Python call per byte into an index per byte.
        """
        buf, pos = self._buf, self._pos
        result = shift = 0
        while True:
            byte = buf[pos]
            pos += 1
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:  # high bit clear: last byte of the varint
                self._pos = pos
                return result
            shift += 7

    def _skip_varint(self) -> None:
        """Step over a varint without reconstructing its value.

        Only the terminator matters when skipping, and it is in-band: the first
        byte with its high bit clear ends the field. Nothing needs the integer,
        so the shift/or work that :meth:`_varint` does is pure waste here --
        which matters because most skipped footer fields are varints.
        """
        buf, pos = self._buf, self._pos
        while buf[pos] & 0x80:
            pos += 1
        self._pos = pos + 1

    def _zigzag(self) -> int:
        """Decode a signed integer from its zigzag-encoded varint.

        A plain varint would make every negative number maximally long, since
        two's complement sets the high bits. Zigzag instead interleaves the signs
        -- 0, -1, 1, -2, 2 encode as 0, 1, 2, 3, 4 -- so magnitude, not sign,
        drives the length.

        Inverting it: ``n >> 1`` recovers the magnitude, and ``-(n & 1)`` is 0 for
        an even ``n`` (a non-negative value, leaving it unchanged) or -1 for an odd
        one. -1 is all-ones, so XOR against it flips every bit, which is exactly
        the two's complement negation ``~x``.
        """
        n = self._varint()
        return (n >> 1) ^ -(n & 1)

    def _list_header(self) -> Tuple[int, int]:
        """Decode a list/set header into ``(element_count, element_type)``.

        One byte packs both: the high nibble is the count and the low nibble the
        element type id. A nibble only holds 0-14, so a count of 15 (``0x0F``) is
        the escape marker meaning "the real count follows as a varint".
        """
        header = self._byte()
        size, element_type = (header >> 4) & 0x0F, header & 0x0F
        return (self._varint() if size == 0x0F else size), element_type

    def _fields(self) -> Iterator[Tuple[int, int]]:
        """Yield ``(field_id, field_type)`` for each field of a struct.

        A field header is one byte: the high nibble is the field id expressed as a
        *delta from the previous field in this struct*, and the low nibble is the
        type id. Since writers emit fields in ascending id order, most deltas are
        1 and fit the nibble, so a whole field header costs a single byte. An
        all-zero byte is the STOP marker ending the struct.

        Two consequences worth knowing:

        - A delta of 0 does not mean "same id"; it is the escape marker saying the
          delta did not fit the nibble, so an explicit zigzag i16 id follows.
        - Because ids are relative, the absolute id depends on every preceding
          field being consumed at exactly the right width. This is why a one-byte
          desync is dangerous: the next delta still decodes, just onto the wrong
          id, silently. Nothing downstream re-derives the answer, so the type
          assertions on recognized fields and the differential test against an
          independent Thrift implementation are what stand between a skip bug and
          wrong block sizes.

        The caller MUST fully consume each field's value before requesting the
        next header -- either by decoding it or by calling :meth:`_skip` -- and
        must never ``break`` out of the loop, or the cursor desyncs.
        """
        buf = self._buf
        last_id = 0
        while True:
            pos = self._pos
            header = buf[pos]
            self._pos = pos + 1
            if header == 0:  # STOP
                return
            delta, field_type = (header >> 4) & 0x0F, header & 0x0F
            last_id = self._zigzag() if delta == 0 else last_id + delta
            yield last_id, field_type

    def _skip_element(self, element_type: int) -> None:
        """Skip one element of a map.

        Lists go through :meth:`_skip_list`, which skips whole runs at a time;
        maps interleave two element types, so they are stepped one at a time and
        are rare enough in a footer not to be worth the same treatment.
        """
        # Inside a container a bool costs one byte; inside a struct it is encoded
        # in the type nibble itself and occupies no payload.
        if element_type in (_BOOL_TRUE, _BOOL_FALSE):
            self._pos += 1
        else:
            self._skip(element_type)

    def _skip(self, field_type: int) -> None:
        """Advance the cursor past one field's value.

        Branches are ordered by how often each type appears in a real footer --
        integers dominate, then the binary blobs in ``Statistics``, then nested
        structs -- since this runs on the large majority of the footer's bytes.
        """
        if field_type in (_I16, _I32, _I64):
            self._skip_varint()
        elif field_type == _BINARY:
            # Deliberately not ``self._pos += self._varint()``: Python evaluates
            # the left operand first, so the advance _varint() performs while
            # reading the length is discarded and every binary field consumes one
            # byte too few. That desync is silent -- the walk then reports
            # plausible but wrong field ids.
            nbytes = self._varint()
            self._pos += nbytes
        elif field_type == _STRUCT:
            self._skip_struct()
        elif field_type in (_LIST, _SET):
            self._skip_list()
        elif field_type in (_BOOL_TRUE, _BOOL_FALSE):
            # A bool in a struct is encoded in the type nibble and has no payload.
            return
        elif field_type == _BYTE:
            self._pos += 1
        elif field_type == _DOUBLE:
            self._pos += 8
        elif field_type == _MAP:
            size = self._varint()
            if size:
                header = self._byte()
                key_type, value_type = (header >> 4) & 0x0F, header & 0x0F
                for _ in range(size):
                    self._skip_element(key_type)
                    self._skip_element(value_type)
        else:
            raise _ThriftDesync(f"unknown compact type {field_type}")

    def _skip_struct(self) -> None:
        """Skip a whole struct without materializing anything about its fields.

        Separate from :meth:`_fields` because skipping needs strictly less than
        traversing: field *ids* are irrelevant when nothing will be read, so the
        long-form id is stepped over rather than decoded, and no ``(id, type)``
        pair is ever built. That matters because this is where the footer's bulk
        goes -- every ``SchemaElement``, every ``Statistics``, every
        ``ColumnIndex`` offset -- and a tuple per field across a wide footer is
        hundreds of thousands of allocations for data we discard.
        """
        buf = self._buf
        while True:
            pos = self._pos
            header = buf[pos]
            pos += 1
            if header == 0:  # STOP
                self._pos = pos
                return
            field_type = header & 0x0F
            if not (header >> 4) & 0x0F:  # long-form id, whose value we discard
                while buf[pos] & 0x80:
                    pos += 1
                pos += 1
            if field_type in (_I16, _I32, _I64):
                # Integers are most of what a footer skips, so this one case is
                # unrolled rather than dispatched through _skip.
                while buf[pos] & 0x80:
                    pos += 1
                self._pos = pos + 1
            else:
                self._pos = pos
                self._skip(field_type)

    def _skip_list(self) -> None:
        """Skip a list or set, in one step where the element width allows it.

        Fixed-width elements need no per-element walk at all, which is what makes
        the level histograms and ``ColumnIndex`` null-page lists cheap to step
        over. Varints have to be scanned individually since their width is in
        the data.
        """
        size, element_type = self._list_header()
        if not size:
            return
        if element_type in (_I16, _I32, _I64):
            for _ in range(size):
                self._skip_varint()
        elif element_type in (_BOOL_TRUE, _BOOL_FALSE, _BYTE):
            # Unlike in a struct, a bool in a container does occupy a byte.
            self._pos += size
        elif element_type == _DOUBLE:
            self._pos += 8 * size
        else:
            for _ in range(size):
                self._skip(element_type)

    def _i64_list(self, field_id: int) -> Tuple[int, ...]:
        size, element_type = self._list_header()
        # An empty list's element type carries no information, so only a
        # populated list is worth checking.
        if size and element_type != _I64:
            raise _ThriftDesync(
                f"SizeStatistics field {field_id} is a list of type "
                f"{element_type}, expected i64"
            )
        return tuple(self._zigzag() for _ in range(size))

    def _size_statistics(self) -> LeafSizeStats:
        unencoded: Optional[int] = None
        repetition: Tuple[int, ...] = ()
        definition: Tuple[int, ...] = ()
        for field_id, field_type in self._fields():
            # Type mismatches on 1/2/3 are the load-bearing check: field 17
            # ``geospatial_statistics`` is also a struct, so a cursor that
            # drifted onto it would pass the outer struct test. Its field 1 is a
            # BoundingBox struct rather than an i64, which only this catches.
            if field_id == _SIZE_STATS_UNENCODED_BYTES:
                if field_type != _I64:
                    raise _ThriftDesync(
                        f"SizeStatistics field 1 has type {field_type}, expected i64"
                    )
                unencoded = self._zigzag()
            elif field_id == _SIZE_STATS_REPETITION_HISTOGRAM:
                if field_type != _LIST:
                    raise _ThriftDesync(
                        f"SizeStatistics field 2 has type {field_type}, expected list"
                    )
                repetition = self._i64_list(field_id)
            elif field_id == _SIZE_STATS_DEFINITION_HISTOGRAM:
                if field_type != _LIST:
                    raise _ThriftDesync(
                        f"SizeStatistics field 3 has type {field_type}, expected list"
                    )
                definition = self._i64_list(field_id)
            else:
                self._skip(field_type)
        return LeafSizeStats(unencoded, repetition, definition)

    def _column_metadata(self) -> Optional[LeafSizeStats]:
        """Read field 16 out of one ``ColumnMetaData``, or ``None`` if absent.

        Hand-inlined rather than driven by :meth:`_fields`, because this is the
        walk's innermost loop -- once per leaf per row group -- and every field
        but one is stepped over. Most of them are integers (offsets, counts,
        sizes), so that case is unrolled here and only the remainder reaches
        :meth:`_skip`. arrow-rs hand-optimized the equivalent structs, and left
        the colder ones declarative, for the same reason.
        """
        size_stats: Optional[LeafSizeStats] = None
        buf = self._buf
        last_id = 0
        while True:
            pos = self._pos
            header = buf[pos]
            self._pos = pos + 1
            if header == 0:  # STOP
                break
            field_type = header & 0x0F
            delta = (header >> 4) & 0x0F
            last_id = last_id + delta if delta else self._zigzag()

            if last_id == _COLUMN_META_SIZE_STATISTICS:
                if field_type != _STRUCT:
                    raise _ThriftDesync(
                        f"ColumnMetaData field 16 has type {field_type}, "
                        "expected struct"
                    )
                size_stats = self._size_statistics()
            elif field_type in (_I16, _I32, _I64):
                self._skip_varint()
            else:
                self._skip(field_type)
        return size_stats

    def _column_chunk(self) -> Optional[LeafSizeStats]:
        stats: Optional[LeafSizeStats] = None
        for field_id, field_type in self._fields():
            if field_id == _COLUMN_CHUNK_META_DATA and field_type == _STRUCT:
                stats = self._column_metadata()
            else:
                self._skip(field_type)
        return stats

    def _row_group(self) -> List[Optional[LeafSizeStats]]:
        columns: List[Optional[LeafSizeStats]] = []
        for field_id, field_type in self._fields():
            if field_id == _ROW_GROUP_COLUMNS and field_type == _LIST:
                size, _ = self._list_header()
                columns = [self._column_chunk() for _ in range(size)]
            else:
                self._skip(field_type)
        return columns

    def read(self) -> List[List[Optional[LeafSizeStats]]]:
        row_groups: List[List[Optional[LeafSizeStats]]] = []
        for field_id, field_type in self._fields():
            if field_id == _FILE_META_ROW_GROUPS and field_type == _LIST:
                size, _ = self._list_header()
                row_groups = [self._row_group() for _ in range(size)]
            else:
                self._skip(field_type)
        return row_groups


def footer_bytes(metadata: "FileMetaData") -> Optional[memoryview]:
    """The raw Thrift footer behind a ``FileMetaData``, or ``None``.

    ``__reduce__`` is how PyArrow pickles ``FileMetaData``, so it returns the
    serialized footer with no additional I/O. It is an implementation detail
    rather than public API, hence probed defensively -- Ray supports
    ``pyarrow >= 17.0.0``.
    """
    try:
        payload = metadata.__reduce__()[1][0]
    except Exception as exc:
        logger.error(f"Error getting footer bytes: {exc}")
        return None
    try:
        # ``memoryview(pyarrow.Buffer)`` has format 'b' -- *signed* char -- so
        # every byte >= 0x80 reads back negative. The reader only ever consumes
        # bytes through masks, which Python's arbitrary-precision two's complement
        # makes sign-agnostic, so this cast is not load-bearing today; it is here
        # so that anything later comparing a raw byte value cannot be caught out.
        return memoryview(payload).cast("B")
    except (TypeError, ValueError) as exc:
        logger.error(f"Error casting footer bytes: {exc}")
        return None


def read_size_statistics(
    metadata: "FileMetaData",
) -> Optional[List[List[Optional[LeafSizeStats]]]]:
    """``SizeStatistics`` indexed ``[row_group][leaf]``, or ``None`` if unusable.

    ``None`` means the caller must fall back to its existing sizing: the footer
    bytes were unrecoverable, or the walk could not produce a result of the shape
    the footer says it should have.

    What guards the result is structural: the type assertions on the fields this
    walk claims to recognize (see :meth:`_CompactReader._size_statistics`), and
    the row-group and leaf counts checked here. The counts matter beyond sanity
    because callers pair ``[row_group][leaf]`` against ``ParquetSchema.column``
    by position, so a wrong leaf count would silently size the wrong columns.

    Note what is *not* checked: the decoded numbers themselves. A cursor desync
    that survives the type assertions and preserves the counts would return
    plausible but wrong sizes rather than ``None``. That is a deliberate trade --
    per-chunk verification cost a decode of two extra fields on every leaf -- and
    it is why the differential test against an independent Thrift implementation
    is the load-bearing check on this module.
    """
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        row_groups = _CompactReader(buf).read()
    except Exception as exc:
        logger.error(f"Error reading size statistics: {exc}")
        # Includes _ThriftDesync and the IndexError a truncated footer produces.
        # Never raises into the caller: sizing has a working fallback, so a
        # footer we cannot decode must not fail the read.
        return None

    if len(row_groups) != metadata.num_row_groups:
        return None
    num_columns = metadata.num_columns
    if any(len(columns) != num_columns for columns in row_groups):
        return None
    return row_groups
