"""Read the Parquet fields that PyArrow's Python bindings don't expose.

:func:`read_size_statistics` reads each column chunk's ``SizeStatistics`` from
the footer bytes ``FileMetaData`` already holds, and :func:`read_dictionary_page`
reads a dictionary page's header. Both decode Thrift's compact protocol by hand,
only the fields they need.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Iterator, List, NamedTuple, Optional, Tuple

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pyarrow.parquet import FileMetaData

# TCompactProtocol type ids.
_BOOL_TRUE, _BOOL_FALSE, _BYTE, _I16, _I32, _I64 = 1, 2, 3, 4, 5, 6
_DOUBLE, _BINARY, _LIST, _SET, _MAP, _STRUCT = 7, 8, 9, 10, 11, 12

# Field ids from parquet.thrift:
# https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
_FILE_META_ROW_GROUPS = 4
_ROW_GROUP_COLUMNS = 1
_COLUMN_CHUNK_META_DATA = 3
_COLUMN_META_SIZE_STATISTICS = 16
_SIZE_STATS_UNENCODED_BYTES = 1
_SIZE_STATS_REPETITION_HISTOGRAM = 2
_SIZE_STATS_DEFINITION_HISTOGRAM = 3
# ``struct PageHeader`` and ``struct DictionaryPageHeader``.
_PAGE_HEADER_TYPE = 1
_PAGE_HEADER_UNCOMPRESSED_SIZE = 2
_PAGE_HEADER_DICTIONARY_PAGE_HEADER = 7
_DICTIONARY_PAGE_HEADER_NUM_VALUES = 1
# ``enum PageType``.
_DICTIONARY_PAGE = 2


class LeafSizeStats(NamedTuple):
    """A column chunk's ``SizeStatistics``: its string bytes and level histograms."""

    unencoded_byte_array_data_bytes: Optional[int]
    repetition_level_histogram: Tuple[int, ...]
    definition_level_histogram: Tuple[int, ...]


class DictionaryPage(NamedTuple):
    """A dictionary page's header: its entries and its uncompressed bytes."""

    num_values: int
    uncompressed_page_size: int


class _ThriftDesync(Exception):
    """A field's type contradicts parquet.thrift: the reader has lost its place."""


class _CompactReader:
    """Reads Thrift's compact protocol, decoding only the fields it needs."""

    __slots__ = ("_buf", "_pos")

    def __init__(self, buf):
        self._buf = buf
        self._pos = 0

    def _byte(self) -> int:
        # Past the end raises IndexError, which read_size_statistics turns into None.
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
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:  # high bit clear: last byte of the varint
                self._pos = pos
                return result
            shift += 7

    def _skip_varint(self) -> None:
        """Step over a varint without decoding it."""
        buf, pos = self._buf, self._pos
        while buf[pos] & 0x80:
            pos += 1
        self._pos = pos + 1

    def _zigzag(self) -> int:
        """A signed varint, zigzag-encoded: 0, -1, 1, -2 are written 0, 1, 2, 3."""
        n = self._varint()
        return (n >> 1) ^ -(n & 1)

    def _list_header(self) -> Tuple[int, int]:
        """A list or set header: ``(count, element type)``. A count of 15 means
        the real count follows as a varint."""
        header = self._byte()
        size, element_type = (header >> 4) & 0x0F, header & 0x0F
        return (self._varint() if size == 0x0F else size), element_type

    def _fields(self) -> Iterator[Tuple[int, int]]:
        """``(field id, type)`` of each field of a struct.

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
            if header == 0:  # STOP
                return
            delta, field_type = (header >> 4) & 0x0F, header & 0x0F
            last_id = self._zigzag() if delta == 0 else last_id + delta
            yield last_id, field_type

    def _skip_element(self, element_type: int) -> None:
        """Step over one element of a map."""
        # A bool takes a byte in a container, none in a struct.
        if element_type in (_BOOL_TRUE, _BOOL_FALSE):
            self._pos += 1
        else:
            self._skip(element_type)

    def _skip(self, field_type: int) -> None:
        """Step over one field's value."""
        if field_type in (_I16, _I32, _I64):
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
        """Step over a struct without decoding its fields' ids: most of a
        footer goes through here."""
        buf = self._buf
        while True:
            pos = self._pos
            header = buf[pos]
            pos += 1
            if header == 0:  # STOP
                self._pos = pos
                return
            field_type = header & 0x0F
            if not (header >> 4) & 0x0F:  # a long-form id
                while buf[pos] & 0x80:
                    pos += 1
                pos += 1
            if field_type in (_I16, _I32, _I64):
                # Integers, the common case, without a call to _skip.
                while buf[pos] & 0x80:
                    pos += 1
                self._pos = pos + 1
            else:
                self._pos = pos
                self._skip(field_type)

    def _skip_list(self) -> None:
        """Step over a list or set, at once when its elements have a fixed width."""
        size, element_type = self._list_header()
        if not size:
            return
        if element_type in (_I16, _I32, _I64):
            for _ in range(size):
                self._skip_varint()
        elif element_type in (_BOOL_TRUE, _BOOL_FALSE, _BYTE):
            # A bool in a container takes a byte.
            self._pos += size
        elif element_type == _DOUBLE:
            self._pos += 8 * size
        else:
            for _ in range(size):
                self._skip(element_type)

    def _i64_list(self, field_id: int) -> Tuple[int, ...]:
        size, element_type = self._list_header()
        # An empty list's element type means nothing.
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
            # The type checks catch a reader that drifted onto field 17,
            # geospatial statistics: a struct too, but its field 1 isn't an i64.
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
        """One ``ColumnMetaData``'s ``SizeStatistics`` (field 16), or ``None``.

        Inlined rather than built on :meth:`_fields`: it runs once per leaf per
        row group.
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


def read_size_statistics(
    metadata: "FileMetaData",
) -> Optional[List[List[Optional[LeafSizeStats]]]]:
    """``SizeStatistics`` by ``[row_group][leaf]``, or ``None`` when the footer
    can't be read or its row-group or leaf counts come out wrong.

    A reader that lost its place but kept the counts would return wrong numbers
    instead; the test against ``thriftpy2`` checks for that.
    """
    buf = footer_bytes(metadata)
    if buf is None:
        return None
    try:
        row_groups = _CompactReader(buf).read()
    except Exception as exc:
        logger.error(f"Error reading size statistics: {exc}")
        # Sizing has a fallback, so a footer this can't decode must not fail the
        # read.
        return None

    if len(row_groups) != metadata.num_row_groups:
        return None
    num_columns = metadata.num_columns
    if any(len(columns) != num_columns for columns in row_groups):
        return None
    return row_groups


# Enough for a dictionary page's header, which takes about 35 bytes.
DICTIONARY_PAGE_HEADER_READ_BYTES = 64


def read_dictionary_page(buf) -> Optional[DictionaryPage]:
    """The dictionary page header ``buf`` starts with, or ``None`` if it doesn't
    start with a whole one."""
    reader = _CompactReader(buf)
    page_type = uncompressed = num_values = None
    try:
        for field_id, field_type in reader._fields():
            if field_id == _PAGE_HEADER_TYPE and field_type == _I32:
                page_type = reader._zigzag()
            elif field_id == _PAGE_HEADER_UNCOMPRESSED_SIZE and field_type == _I32:
                uncompressed = reader._zigzag()
            elif (
                field_id == _PAGE_HEADER_DICTIONARY_PAGE_HEADER
                and field_type == _STRUCT
            ):
                for sub_id, sub_type in reader._fields():
                    if (
                        sub_id == _DICTIONARY_PAGE_HEADER_NUM_VALUES
                        and sub_type == _I32
                    ):
                        num_values = reader._zigzag()
                    else:
                        reader._skip(sub_type)
            else:
                reader._skip(field_type)
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
