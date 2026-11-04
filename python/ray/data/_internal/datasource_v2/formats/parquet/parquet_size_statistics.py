"""Read the Parquet fields that PyArrow's Python bindings don't expose.

A Parquet file has one footer, at its end, with metadata for every leaf of every
row group. A leaf is a column of primitive values: a flat column is one leaf, and
a struct column of two fields is two. :func:`read_footer_stats` reads each
leaf's ``SizeStatistics`` and data page count from the footer bytes
``FileMetaData`` already holds, and :func:`read_dictionary_page` reads a
dictionary page's header. Both decode Thrift's compact protocol by hand, only
the fields they need.

:func:`read_footer_stats` runs the compiled walk in ``_compact_reader.pyx``:
0.35 s for 1,000 row groups of 1,000 leaves (Apple M5, Python 3.12), 40% of it
PyArrow re-serializing the footer. A source tree Bazel hasn't built, such as one
``setup-dev.py`` links in, runs :class:`_CompactReader`, the same walk in Python,
in 6 s. The result holds about 185 bytes per row group per leaf; the footer bytes
take up to twice the footer's size in PyArrow's memory pool while it runs.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)

try:
    # Absent from a source tree Bazel hasn't built, such as one setup-dev.py links in.
    from ray.data._internal.datasource_v2.formats.parquet._compact_reader import (
        CompactReader as _CompiledCompactReader,
    )
except ImportError:
    _CompiledCompactReader = None

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
_COLUMN_META_ENCODING_STATS = 13  # ColumnMetaData.encoding_stats, a list
_COLUMN_META_SIZE_STATISTICS = 16  # ColumnMetaData.size_statistics
_ENCODING_STATS_PAGE_TYPE = 1  # PageEncodingStats.page_type, a PageType
_ENCODING_STATS_COUNT = 3  # PageEncodingStats.count
_SIZE_STATS_UNENCODED_BYTES = 1  # SizeStatistics.unencoded_byte_array_data_bytes
_SIZE_STATS_REPETITION_HISTOGRAM = 2  # SizeStatistics.repetition_level_histogram
_SIZE_STATS_DEFINITION_HISTOGRAM = 3  # SizeStatistics.definition_level_histogram
_PAGE_HEADER_TYPE = 1  # PageHeader.type, a PageType
_PAGE_HEADER_UNCOMPRESSED_SIZE = 2  # PageHeader.uncompressed_page_size
_PAGE_HEADER_DICTIONARY_PAGE_HEADER = 7  # PageHeader.dictionary_page_header
_DICTIONARY_PAGE_HEADER_NUM_VALUES = 1  # DictionaryPageHeader.num_values
_DATA_PAGE = 0  # PageType.DATA_PAGE
_DICTIONARY_PAGE = 2  # PageType.DICTIONARY_PAGE
_DATA_PAGE_V2 = 3  # PageType.DATA_PAGE_V2


class LeafSizeStats(NamedTuple):
    """A leaf's ``SizeStatistics`` in a row group: string bytes, level histograms."""

    unencoded_byte_array_data_bytes: Optional[int]
    repetition_level_histogram: Tuple[int, ...]
    definition_level_histogram: Tuple[int, ...]


class FooterStats(NamedTuple):
    """What :func:`read_footer_stats` reads, each by ``[row_group][leaf]``."""

    size_statistics: List[List[Optional[LeafSizeStats]]]
    # From ``encoding_stats``; ``None`` where it counts no data pages.
    data_pages: List[List[Optional[int]]]


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
    """A field's type contradicts parquet.thrift: the reader has lost its place."""


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
        """An unsigned varint: 7 bits a byte, low bits first, the high bit set
        on every byte but the last."""
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

    def _list_header(self) -> _ListHeader:
        """A list or set header."""
        header = self._byte()
        count = (header >> _NIBBLE_BITS) & _NIBBLE_MASK
        if count == _LONG_FORM_COUNT:
            count = self._varint()
        return _ListHeader(count, header & _NIBBLE_MASK)

    def _fields(self) -> Iterator[_FieldHeader]:
        """The header of each field of a struct.

        Each id is a delta from the one before, so the caller must consume every
        field's value, by decoding it or :meth:`_skip`, before asking for the
        next, and must not break out of the loop.
        """
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
            count = self._varint()
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
        """Step over a struct without decoding its fields' ids: most of a
        footer goes through here."""
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
        return tuple(self._zigzag() for _ in range(header.count))

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

    def _data_pages(self) -> int:
        """The data pages an ``encoding_stats`` list counts."""
        header = self._list_header()
        if header.count and header.element_type != _STRUCT:
            raise _ThriftDesync(
                f"ColumnMetaData field 13 is a list of type {header.element_type}, "
                "expected struct"
            )
        pages = 0
        for _ in range(header.count):
            # Each entry counts the pages of one type and encoding.
            page_type, count = None, 0
            for field in self._fields():
                if field.id == _ENCODING_STATS_PAGE_TYPE and field.type == _I32:
                    page_type = self._zigzag()
                elif field.id == _ENCODING_STATS_COUNT and field.type == _I32:
                    count = self._zigzag()
                else:
                    self._skip(field.type)
            if page_type in (_DATA_PAGE, _DATA_PAGE_V2):
                pages += count
        return pages

    def _column_metadata(self) -> Tuple[Optional[LeafSizeStats], Optional[int]]:
        """One ``ColumnMetaData``'s ``SizeStatistics`` (field 16) and data page
        count (field 13), each ``None`` without it.

        Inlined rather than built on :meth:`_fields`: it runs once per leaf per
        row group.
        """
        size_stats: Optional[LeafSizeStats] = None
        pages = 0
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
            elif last_id == _COLUMN_META_ENCODING_STATS:
                if field_type != _LIST:
                    raise _ThriftDesync(
                        f"ColumnMetaData field 13 has type {field_type}, expected list"
                    )
                pages = self._data_pages()
            elif field_type in _VARINT_TYPES:
                self._skip_varint()
            else:
                self._skip(field_type)
        # No data pages counted means no count to go by.
        return size_stats, pages if pages > 0 else None

    def _column_chunk(self) -> Tuple[Optional[LeafSizeStats], Optional[int]]:
        leaf: Tuple[Optional[LeafSizeStats], Optional[int]] = (None, None)
        for field in self._fields():
            if field.id == _COLUMN_CHUNK_META_DATA and field.type == _STRUCT:
                leaf = self._column_metadata()
            else:
                self._skip(field.type)
        return leaf

    def _row_group(self) -> Tuple[List[Optional[LeafSizeStats]], List[Optional[int]]]:
        stats: List[Optional[LeafSizeStats]] = []
        pages: List[Optional[int]] = []
        for field in self._fields():
            if field.id == _ROW_GROUP_COLUMNS and field.type == _LIST:
                for _ in range(self._list_header().count):
                    leaf_stats, leaf_pages = self._column_chunk()
                    stats.append(leaf_stats)
                    pages.append(leaf_pages)
            else:
                self._skip(field.type)
        return stats, pages

    def read(
        self,
    ) -> Tuple[List[List[Optional[LeafSizeStats]]], List[List[Optional[int]]]]:
        """``SizeStatistics`` and data page counts, each by ``[row_group][leaf]``."""
        stats: List[List[Optional[LeafSizeStats]]] = []
        pages: List[List[Optional[int]]] = []
        for field in self._fields():
            if field.id == _FILE_META_ROW_GROUPS and field.type == _LIST:
                for _ in range(self._list_header().count):
                    row_group_stats, row_group_pages = self._row_group()
                    stats.append(row_group_stats)
                    pages.append(row_group_pages)
            else:
                self._skip(field.type)
        return stats, pages


def footer_bytes(metadata: "FileMetaData") -> Optional[memoryview]:
    """The raw Thrift footer behind a ``FileMetaData``, or ``None``.

    From ``__reduce__``, which PyArrow pickles it with: no I/O, but not public
    API either.
    """
    try:
        payload = metadata.__reduce__()[1][0]
    except Exception as exc:
        logger.error(f"Error getting footer bytes: {exc}")
        return None
    try:
        # Unsigned: ``memoryview(pyarrow.Buffer)`` gives signed bytes.
        return memoryview(payload).cast("B")
    except (TypeError, ValueError) as exc:
        logger.error(f"Error casting footer bytes: {exc}")
        return None


def read_footer_stats(metadata: "FileMetaData") -> Optional[FooterStats]:
    """Each leaf's ``SizeStatistics`` and data page count, or ``None`` when the
    footer can't be read or its row-group or leaf counts come out wrong.

    A reader that lost its place but kept the counts would return wrong numbers
    instead; the test against ``thriftpy2`` checks for that.
    """
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        if _CompiledCompactReader is not None:
            row_groups, pages = _CompiledCompactReader(buf, LeafSizeStats).read()
        else:
            row_groups, pages = _CompactReader(buf).read()
    except Exception as exc:
        logger.error(f"Error reading footer stats: {exc}")
        # Sizing has a fallback, so a footer this can't decode must not fail the
        # read.
        return None

    if len(row_groups) != metadata.num_row_groups:
        return None
    num_columns = metadata.num_columns
    if any(len(columns) != num_columns for columns in row_groups):
        return None
    # The walk fills ``pages`` alongside, so its counts match.
    return FooterStats(row_groups, pages)


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
    except (IndexError, _ThriftDesync):
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
