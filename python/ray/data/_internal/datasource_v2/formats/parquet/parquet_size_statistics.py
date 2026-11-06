"""Read the Parquet fields that PyArrow's Python bindings don't expose.

:func:`read_footer_stats` reads each leaf's ``SizeStatistics`` from the footer bytes
``FileMetaData`` already holds, and :func:`read_dictionary_page` a dictionary page's
header, decoding Thrift's compact protocol by hand. A leaf is a column of primitive
values: a struct column of two fields is two. The footer walk takes about 7
microseconds per row group per leaf (Apple M5) and its result holds about 190 bytes
each.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterator, List, NamedTuple, Optional, Tuple, Union

from ray.util.debug import log_once

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData

# Thrift's compact protocol, which Parquet footers and page headers are written in:
# https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

# Type ids, and what follows a field header of each type.
_BOOL_TRUE = 1  # a bool field holding true: nothing, the type id is the value
_BOOL_FALSE = 2  # a bool field holding false: nothing
_BYTE = 3  # one byte
_I16 = 4  # a zigzag varint
_I32 = 5  # a zigzag varint
_I64 = 6  # a zigzag varint
_DOUBLE = 7  # an 8-byte float
_BINARY = 8  # a varint length, then that many bytes
_LIST = 9  # a list header, then the elements
_SET = 10  # written like a list
_MAP = 11  # a varint count, then a byte holding the key and value type ids
_STRUCT = 12  # fields, then a STOP byte
_DOUBLE_BYTES = 8  # the width of a double
_VARINT_TYPES = frozenset((_I16, _I32, _I64))  # types whose payload is one varint

# A field header byte holds the field id's delta from the previous field's in
# its high 4 bits and the field's type id in its low 4; a list header byte holds
# the element count and the element type id the same way.
_NIBBLE_BITS = 4  # shift a header byte right by this for its high 4 bits
_NIBBLE_MASK = 0x0F  # mask a header byte with this for its low 4 bits
_LONG_FORM_ID = 0  # an id delta of 0: the field id follows as a zigzag varint
_LONG_FORM_COUNT = 0x0F  # a list count of 15: the count follows as a varint
_STOP = 0x00  # the byte ending a struct; never a field header, as types start at 1

# Varints: unsigned integers written 7 bits a byte, low bits first.
_VARINT_BITS = 7  # value bits in each byte of a varint
_VARINT_VALUE_MASK = 0x7F  # selects a varint byte's 7 value bits
_VARINT_CONTINUES = 0x80  # a varint byte's high bit, set on every byte but the last

# Field ids from parquet.thrift:
# https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
_FILE_META_ROW_GROUPS = 4  # FileMetaData.row_groups, list<RowGroup>
_ROW_GROUP_COLUMNS = 1  # RowGroup.columns, list<ColumnChunk>
_COLUMN_CHUNK_META_DATA = 3  # ColumnChunk.meta_data, a ColumnMetaData
_COLUMN_META_SIZE_STATISTICS = 16  # ColumnMetaData.size_statistics
_SIZE_STATS_UNENCODED_BYTES = 1  # SizeStatistics.unencoded_byte_array_data_bytes
_SIZE_STATS_REPETITION_HISTOGRAM = 2  # SizeStatistics.repetition_level_histogram
_SIZE_STATS_DEFINITION_HISTOGRAM = 3  # SizeStatistics.definition_level_histogram
_PAGE_HEADER_TYPE = 1  # PageHeader.type, a PageType
_PAGE_HEADER_UNCOMPRESSED_SIZE = 2  # PageHeader.uncompressed_page_size
_PAGE_HEADER_DICTIONARY_PAGE_HEADER = 7  # PageHeader.dictionary_page_header
_DICTIONARY_PAGE_HEADER_NUM_VALUES = 1  # DictionaryPageHeader.num_values
_DICTIONARY_PAGE = 2  # PageType.DICTIONARY_PAGE


class LeafSizeStats(NamedTuple):
    """A leaf's ``SizeStatistics`` in a row group: string bytes, level histograms."""

    unencoded_byte_array_data_bytes: Optional[int]
    repetition_level_histogram: Tuple[int, ...]
    definition_level_histogram: Tuple[int, ...]


class DictionaryPage(NamedTuple):
    """A dictionary page's header: its entries and its uncompressed bytes."""

    num_values: int
    uncompressed_page_size: int


@dataclass(slots=True)
class _FieldHeader:
    """The id and type id a struct field starts with."""

    id: int
    type: int


@dataclass(slots=True)
class _ListHeader:
    """The element count and element type id a list or set starts with."""

    count: int
    element_type: int


class _ThriftDesync(Exception):
    """A field's type or a count is impossible: the reader has lost its place."""


class _CompactReader:
    """Reads Thrift's compact protocol, decoding only the fields it needs."""

    __slots__ = ("_buf", "_pos")

    def __init__(self, buf: Union[bytes, memoryview]):
        self._buf = buf
        self._pos = 0

    def _byte(self) -> int:
        # Past the end raises IndexError, which read_footer_stats turns into None.
        value = self._buf[self._pos]
        self._pos += 1
        return value

    def _varint(self) -> int:
        """An unsigned varint."""
        buf, pos = self._buf, self._pos
        result = shift = 0
        while True:
            byte = buf[pos]
            pos += 1
            result |= (byte & _VARINT_VALUE_MASK) << shift
            if not byte & _VARINT_CONTINUES:
                self._pos = pos
                return result
            shift += _VARINT_BITS

    def _skip_varint(self) -> None:
        """Step over a varint without decoding it."""
        buf, pos = self._buf, self._pos
        while buf[pos] & _VARINT_CONTINUES:
            pos += 1
        self._pos = pos + 1

    def _zigzag(self) -> int:
        """A signed varint, zigzag-encoded: 0, -1, 1, -2 are written 0, 1, 2, 3."""
        n = self._varint()
        return (n >> 1) ^ -(n & 1)

    def _element_count(self) -> int:
        """A container's varint element count, no more than the bytes left."""
        count = self._varint()
        # Every element takes a byte at least. A corrupt count must raise here:
        # skipping fixed-width elements reads nothing, so it would loop instead.
        if count > len(self._buf) - self._pos:
            raise _ThriftDesync(f"{count} elements run past the buffer")
        return count

    def _list_header(self) -> _ListHeader:
        """A list or set header."""
        header = self._byte()
        count = (header >> _NIBBLE_BITS) & _NIBBLE_MASK
        if count == _LONG_FORM_COUNT:
            count = self._element_count()
        return _ListHeader(count, header & _NIBBLE_MASK)

    def _fields(self) -> Iterator[_FieldHeader]:
        """Each field's header; consume its value before the next, and never break."""
        buf = self._buf
        last_id = 0
        while True:
            pos = self._pos
            header = buf[pos]
            self._pos = pos + 1
            if header == _STOP:
                return
            delta = (header >> _NIBBLE_BITS) & _NIBBLE_MASK
            last_id = self._zigzag() if delta == _LONG_FORM_ID else last_id + delta
            yield _FieldHeader(last_id, header & _NIBBLE_MASK)

    def _skip_element(self, element_type: int) -> None:
        """Step over one element of a map."""
        # A bool takes a byte in a container, none in a struct.
        if element_type in (_BOOL_TRUE, _BOOL_FALSE):
            self._pos += 1
        else:
            self._skip(element_type)

    def _skip(self, field_type: int) -> None:
        """Step over one field's value."""
        if field_type in _VARINT_TYPES:
            self._skip_varint()
        elif field_type == _BINARY:
            # Not ``self._pos += self._varint()``, which reads ``self._pos``
            # before _varint() moves it and so skips one byte too few.
            nbytes = self._varint()
            self._pos += nbytes
        elif field_type == _STRUCT:
            self._skip_struct()
        elif field_type in (_LIST, _SET):
            self._skip_list()
        elif field_type in (_BOOL_TRUE, _BOOL_FALSE):
            # A bool in a struct has no payload.
            return
        elif field_type == _BYTE:
            self._pos += 1
        elif field_type == _DOUBLE:
            self._pos += _DOUBLE_BYTES
        elif field_type == _MAP:
            count = self._element_count()
            if count:
                header = self._byte()
                key_type = (header >> _NIBBLE_BITS) & _NIBBLE_MASK
                value_type = header & _NIBBLE_MASK
                for _ in range(count):
                    self._skip_element(key_type)
                    self._skip_element(value_type)
        else:
            raise _ThriftDesync(f"unknown compact type {field_type}")

    def _skip_struct(self) -> None:
        """Step over a struct; most of a footer goes through here."""
        buf = self._buf
        while True:
            pos = self._pos
            header = buf[pos]
            pos += 1
            if header == _STOP:
                self._pos = pos
                return
            field_type = header & _NIBBLE_MASK
            if (header >> _NIBBLE_BITS) & _NIBBLE_MASK == _LONG_FORM_ID:
                while buf[pos] & _VARINT_CONTINUES:
                    pos += 1
                pos += 1
            if field_type in _VARINT_TYPES:
                # Integers, the common case, without a call to _skip.
                while buf[pos] & _VARINT_CONTINUES:
                    pos += 1
                self._pos = pos + 1
            else:
                self._pos = pos
                self._skip(field_type)

    def _skip_list(self) -> None:
        """Step over a list or set, at once when its elements have a fixed width."""
        header = self._list_header()
        count, element_type = header.count, header.element_type
        if not count:
            return
        if element_type in _VARINT_TYPES:
            for _ in range(count):
                self._skip_varint()
        elif element_type in (_BOOL_TRUE, _BOOL_FALSE, _BYTE):
            # A bool in a container takes a byte.
            self._pos += count
        elif element_type == _DOUBLE:
            self._pos += _DOUBLE_BYTES * count
        else:
            for _ in range(count):
                self._skip(element_type)

    def _i64_list(self, field_id: int) -> Tuple[int, ...]:
        header = self._list_header()
        # An empty list's element type means nothing.
        if header.count and header.element_type != _I64:
            raise _ThriftDesync(
                f"SizeStatistics field {field_id} is a list of type "
                f"{header.element_type}, expected i64"
            )
        counts = tuple(self._zigzag() for _ in range(header.count))
        if counts and min(counts) < 0:
            raise _ThriftDesync(f"SizeStatistics field {field_id} has a negative count")
        return counts

    def _size_statistics(self) -> LeafSizeStats:
        unencoded: Optional[int] = None
        repetition: Tuple[int, ...] = ()
        definition: Tuple[int, ...] = ()
        for field in self._fields():
            # The type checks catch a reader that drifted onto field 17,
            # geospatial statistics: a struct too, but its field 1 isn't an i64.
            if field.id == _SIZE_STATS_UNENCODED_BYTES:
                if field.type != _I64:
                    raise _ThriftDesync(
                        f"SizeStatistics field 1 has type {field.type}, expected i64"
                    )
                unencoded = self._zigzag()
                if unencoded < 0:
                    raise _ThriftDesync("SizeStatistics field 1 is negative")
            elif field.id == _SIZE_STATS_REPETITION_HISTOGRAM:
                if field.type != _LIST:
                    raise _ThriftDesync(
                        f"SizeStatistics field 2 has type {field.type}, expected list"
                    )
                repetition = self._i64_list(field.id)
            elif field.id == _SIZE_STATS_DEFINITION_HISTOGRAM:
                if field.type != _LIST:
                    raise _ThriftDesync(
                        f"SizeStatistics field 3 has type {field.type}, expected list"
                    )
                definition = self._i64_list(field.id)
            else:
                self._skip(field.type)
        return LeafSizeStats(unencoded, repetition, definition)

    def _column_metadata(self) -> Optional[LeafSizeStats]:
        """A ``ColumnMetaData``'s ``SizeStatistics``, ``None`` if absent."""
        # Inlined rather than built on _fields: it runs once per leaf per row group.
        size_stats: Optional[LeafSizeStats] = None
        buf = self._buf
        last_id = 0
        while True:
            pos = self._pos
            header = buf[pos]
            self._pos = pos + 1
            if header == _STOP:
                break
            field_type = header & _NIBBLE_MASK
            delta = (header >> _NIBBLE_BITS) & _NIBBLE_MASK
            last_id = self._zigzag() if delta == _LONG_FORM_ID else last_id + delta

            if last_id == _COLUMN_META_SIZE_STATISTICS:
                if field_type != _STRUCT:
                    raise _ThriftDesync(
                        f"ColumnMetaData field 16 has type {field_type}, "
                        "expected struct"
                    )
                size_stats = self._size_statistics()
            elif field_type in _VARINT_TYPES:
                self._skip_varint()
            else:
                self._skip(field_type)
        return size_stats

    def _column_chunk(self) -> Optional[LeafSizeStats]:
        leaf = None
        for field in self._fields():
            if field.id == _COLUMN_CHUNK_META_DATA and field.type == _STRUCT:
                leaf = self._column_metadata()
            else:
                self._skip(field.type)
        return leaf

    def _row_group(self) -> List[Optional[LeafSizeStats]]:
        stats: List[Optional[LeafSizeStats]] = []
        for field in self._fields():
            if field.id == _ROW_GROUP_COLUMNS and field.type == _LIST:
                for _ in range(self._list_header().count):
                    stats.append(self._column_chunk())
            else:
                self._skip(field.type)
        return stats

    def read(self) -> List[List[Optional[LeafSizeStats]]]:
        """``SizeStatistics`` by ``[row_group][leaf]``."""
        stats: List[List[Optional[LeafSizeStats]]] = []
        for field in self._fields():
            if field.id == _FILE_META_ROW_GROUPS and field.type == _LIST:
                for _ in range(self._list_header().count):
                    stats.append(self._row_group())
            else:
                self._skip(field.type)
        # Bytes that aren't exactly one FileMetaData aren't the footer.
        if self._pos != len(self._buf):
            raise _ThriftDesync("bytes left after the footer")
        return stats


def footer_bytes(metadata: "FileMetaData") -> Optional[memoryview]:
    """A ``FileMetaData``'s Thrift footer, or ``None``: from ``__reduce__``, which
    PyArrow pickles it with by writing it out again, so no I/O but not public API."""
    try:
        # Unsigned: ``memoryview(pyarrow.Buffer)`` gives signed bytes.
        return memoryview(metadata.__reduce__()[1][0]).cast("B")
    except Exception as exc:
        # Once per process: a PyArrow release that changes __reduce__ fails
        # every file the same way.
        if log_once("parquet_footer_bytes_unavailable"):
            logger.warning(
                "Can't get Parquet footer bytes from PyArrow's FileMetaData, so "
                "SizeStatistics won't be read: %s",
                exc,
            )
        return None


def read_footer_stats(
    metadata: "FileMetaData",
) -> Optional[List[List[Optional[LeafSizeStats]]]]:
    """Each leaf's ``SizeStatistics`` by ``[row_group][leaf]``, or ``None`` when the
    footer can't be read or its row-group or leaf counts come out wrong."""
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        row_groups = _CompactReader(buf).read()
        num_row_groups = metadata.num_row_groups
        num_columns = metadata.num_columns
    except Exception as exc:
        # Sizing falls back, so a footer this can't decode must not fail the read.
        if log_once("parquet_footer_stats_unreadable"):
            logger.warning(
                "Can't read SizeStatistics from a Parquet footer; files that "
                "fail like this are sized without them: %s",
                exc,
            )
        return None

    if len(row_groups) != num_row_groups:
        return None
    if any(len(columns) != num_columns for columns in row_groups):
        return None
    return row_groups


# Enough for a dictionary page's header, which takes about 35 bytes.
DICTIONARY_PAGE_HEADER_READ_BYTES = 64


def read_dictionary_page(buf: Union[bytes, memoryview]) -> Optional[DictionaryPage]:
    """The dictionary page header ``buf`` starts with, or ``None`` if it doesn't
    start with a whole one."""
    reader = _CompactReader(buf)
    page_type = uncompressed = num_values = None
    try:
        for field in reader._fields():
            if field.id == _PAGE_HEADER_TYPE and field.type == _I32:
                page_type = reader._zigzag()
            elif field.id == _PAGE_HEADER_UNCOMPRESSED_SIZE and field.type == _I32:
                uncompressed = reader._zigzag()
            elif (
                field.id == _PAGE_HEADER_DICTIONARY_PAGE_HEADER
                and field.type == _STRUCT
            ):
                for sub in reader._fields():
                    if (
                        sub.id == _DICTIONARY_PAGE_HEADER_NUM_VALUES
                        and sub.type == _I32
                    ):
                        num_values = reader._zigzag()
                    else:
                        reader._skip(sub.type)
            else:
                reader._skip(field.type)
    except (IndexError, RecursionError, _ThriftDesync):
        # RecursionError: structs nested deeper than the stack allows.
        return None
    if (
        page_type != _DICTIONARY_PAGE
        or uncompressed is None
        or num_values is None
        or num_values < 0
        or uncompressed < 0
    ):
        return None
    return DictionaryPage(num_values, uncompressed)
