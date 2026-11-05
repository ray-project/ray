# cython: language_level=3, boundscheck=True, wraparound=False, initializedcheck=False
"""Thrift compact-protocol reader for :mod:`parquet_size_statistics`.

Reads a Parquet footer's ``SizeStatistics`` and data page counts, and a dictionary
page's header, decoding only those fields. Bazel builds it into Ray's wheels; see
python/ray/data/BUILD.bazel.
"""
from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from libc.stdint cimport int64_t, uint64_t

# Thrift's compact protocol, which Parquet footers and page headers are written in:
# https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
cdef enum:
    # Type ids, and what follows a field header of each type.
    BOOL_TRUE = 1  # a bool field holding true: nothing, the type id is the value
    BOOL_FALSE = 2  # a bool field holding false: nothing
    BYTE = 3  # one byte
    I16 = 4  # a zigzag varint
    I32 = 5  # a zigzag varint
    I64 = 6  # a zigzag varint
    DOUBLE = 7  # an 8-byte float
    BINARY = 8  # a varint length, then that many bytes
    LIST = 9  # a list header, then the elements
    SET = 10  # written like a list
    MAP = 11  # a varint count, then a byte holding the key and value type ids
    STRUCT = 12  # fields, then a STOP byte
    DOUBLE_BYTES = 8  # the width of a double

    # A field header byte holds the field id's delta from the previous field's in
    # its high 4 bits and the field's type id in its low 4; a list header byte
    # holds the element count and the element type id the same way.
    NIBBLE_BITS = 4
    NIBBLE_MASK = 0x0F
    LONG_FORM_ID = 0  # an id delta of 0: the field id follows as a zigzag varint
    LONG_FORM_COUNT = 0x0F  # a list count of 15: the count follows as a varint
    STOP = 0x00  # the byte ending a struct; never a field header, as types start at 1

    # Varints: unsigned integers written 7 bits a byte, low bits first.
    VARINT_BITS = 7
    VARINT_VALUE_MASK = 0x7F
    VARINT_CONTINUES = 0x80  # set on every byte of a varint but the last

    # Field ids from parquet.thrift:
    # https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
    FILE_META_ROW_GROUPS = 4  # FileMetaData.row_groups, list<RowGroup>
    ROW_GROUP_COLUMNS = 1  # RowGroup.columns, list<ColumnChunk>
    COLUMN_CHUNK_META_DATA = 3  # ColumnChunk.meta_data, a ColumnMetaData
    COLUMN_META_ENCODING_STATS = 13  # ColumnMetaData.encoding_stats, a list
    COLUMN_META_SIZE_STATISTICS = 16  # ColumnMetaData.size_statistics
    ENCODING_STATS_PAGE_TYPE = 1  # PageEncodingStats.page_type, a PageType
    ENCODING_STATS_COUNT = 3  # PageEncodingStats.count
    SIZE_STATS_UNENCODED_BYTES = 1  # SizeStatistics.unencoded_byte_array_data_bytes
    SIZE_STATS_REPETITION_HISTOGRAM = 2  # SizeStatistics.repetition_level_histogram
    SIZE_STATS_DEFINITION_HISTOGRAM = 3  # SizeStatistics.definition_level_histogram
    PAGE_HEADER_TYPE = 1  # PageHeader.type, a PageType
    PAGE_HEADER_UNCOMPRESSED_SIZE = 2  # PageHeader.uncompressed_page_size
    PAGE_HEADER_DICTIONARY_PAGE_HEADER = 7  # PageHeader.dictionary_page_header
    DICTIONARY_PAGE_HEADER_NUM_VALUES = 1  # DictionaryPageHeader.num_values
    DATA_PAGE = 0  # PageType.DATA_PAGE
    DICTIONARY_PAGE = 2  # PageType.DICTIONARY_PAGE
    DATA_PAGE_V2 = 3  # PageType.DATA_PAGE_V2


class ThriftDesync(Exception):
    """A field's type contradicts parquet.thrift: the reader has lost its place."""


cdef inline bint is_varint_type(int t):
    return t == I16 or t == I32 or t == I64


cdef class CompactReader:
    """Reads a footer's ``SizeStatistics``, into ``leaf_stats`` tuples, and data
    page counts, or a dictionary page's header.

    Past the end raises IndexError.
    """

    cdef const unsigned char[::1] buf
    cdef Py_ssize_t pos
    cdef object leaf_stats
    cdef object tuple_new

    def __cinit__(self, buf, leaf_stats=None):
        # Unsigned whatever the view's format: memoryview(pyarrow.Buffer) is signed.
        self.buf = memoryview(buf).cast("B")
        self.pos = 0
        self.leaf_stats = leaf_stats
        # Builds a namedtuple without the Python call in its own __new__.
        self.tuple_new = tuple.__new__

    cdef inline int byte(self) except -1:
        cdef int value = self.buf[self.pos]
        self.pos += 1
        return value

    cdef uint64_t varint(self) except? 0:
        cdef uint64_t result = 0
        cdef int shift = 0
        cdef unsigned char b
        while True:
            b = self.buf[self.pos]
            self.pos += 1
            if shift > 63:
                raise ThriftDesync("varint longer than 64 bits")
            result |= (<uint64_t>(b & VARINT_VALUE_MASK)) << shift
            if not (b & VARINT_CONTINUES):
                return result
            shift += VARINT_BITS

    cdef Py_ssize_t length(self) except -1:
        """A varint count or byte length, which can't exceed the bytes left."""
        cdef uint64_t n = self.varint()
        # A corrupt one could otherwise cast negative and move pos backwards.
        if n > <uint64_t>(self.buf.shape[0] - self.pos):
            raise IndexError("a count runs past the footer")
        return <Py_ssize_t>n

    cdef inline int skip_varint(self) except -1:
        while self.buf[self.pos] & VARINT_CONTINUES:
            self.pos += 1
        self.pos += 1
        return 0

    cdef int64_t zigzag(self) except? -1:
        cdef uint64_t n = self.varint()
        return <int64_t>((n >> 1) ^ (-(n & 1)))

    cdef int list_header(self, Py_ssize_t* count, int* element_type) except -1:
        cdef int header = self.byte()
        cdef Py_ssize_t c = (header >> NIBBLE_BITS) & NIBBLE_MASK
        if c == LONG_FORM_COUNT:
            c = self.length()
        count[0] = c
        element_type[0] = header & NIBBLE_MASK
        return 0

    cdef int next_field(self, int64_t* last_id, int* field_type) except -1:
        """1 with the next field's id and type, or 0 at the struct's STOP byte."""
        cdef int header = self.byte()
        cdef int delta
        if header == STOP:
            return 0
        delta = (header >> NIBBLE_BITS) & NIBBLE_MASK
        if delta == LONG_FORM_ID:
            last_id[0] = self.zigzag()
        else:
            last_id[0] += delta
        field_type[0] = header & NIBBLE_MASK
        return 1

    cdef int skip_element(self, int element_type) except -1:
        # A bool takes a byte in a container, none in a struct.
        if element_type == BOOL_TRUE or element_type == BOOL_FALSE:
            self.pos += 1
        else:
            self.skip(element_type)
        return 0

    cdef int skip(self, int field_type) except -1:
        cdef Py_ssize_t count, i
        cdef int header, key_type, value_type
        if is_varint_type(field_type):
            self.skip_varint()
        elif field_type == BINARY:
            self.pos += self.length()
        elif field_type == STRUCT:
            self.skip_struct()
        elif field_type == LIST or field_type == SET:
            self.skip_list()
        elif field_type == BOOL_TRUE or field_type == BOOL_FALSE:
            pass
        elif field_type == BYTE:
            self.pos += 1
        elif field_type == DOUBLE:
            self.pos += DOUBLE_BYTES
        elif field_type == MAP:
            count = self.length()
            if count:
                header = self.byte()
                key_type = (header >> NIBBLE_BITS) & NIBBLE_MASK
                value_type = header & NIBBLE_MASK
                for i in range(count):
                    self.skip_element(key_type)
                    self.skip_element(value_type)
        else:
            raise ThriftDesync(f"unknown compact type {field_type}")
        return 0

    cdef int skip_struct(self) except -1:
        cdef int header, field_type
        while True:
            header = self.byte()
            if header == STOP:
                return 0
            field_type = header & NIBBLE_MASK
            if ((header >> NIBBLE_BITS) & NIBBLE_MASK) == LONG_FORM_ID:
                self.skip_varint()
            if is_varint_type(field_type):
                self.skip_varint()
            else:
                self.skip(field_type)

    cdef int skip_list(self) except -1:
        cdef Py_ssize_t count, i
        cdef int element_type
        self.list_header(&count, &element_type)
        if not count:
            return 0
        if is_varint_type(element_type):
            for i in range(count):
                self.skip_varint()
        elif element_type == BOOL_TRUE or element_type == BOOL_FALSE or element_type == BYTE:
            self.pos += count
        elif element_type == DOUBLE:
            self.pos += DOUBLE_BYTES * count
        else:
            for i in range(count):
                self.skip(element_type)
        return 0

    cdef tuple i64_list(self, int64_t field_id):
        cdef Py_ssize_t count, i
        cdef int element_type
        cdef object value
        cdef tuple values
        self.list_header(&count, &element_type)
        # An empty list's element type means nothing.
        if count and element_type != I64:
            raise ThriftDesync(
                f"SizeStatistics field {field_id} is a list of type {element_type}, "
                "expected i64"
            )
        # Each element takes at least a byte: don't allocate for a corrupt count.
        if count > self.buf.shape[0] - self.pos:
            raise IndexError("list runs past the footer")
        values = PyTuple_New(count)
        for i in range(count):
            value = self.zigzag()
            Py_INCREF(value)
            PyTuple_SET_ITEM(values, i, value)
        return values

    cdef object size_statistics(self):
        cdef int64_t last_id = 0
        cdef int field_type
        unencoded = None
        repetition = ()
        definition = ()
        while self.next_field(&last_id, &field_type):
            # The type checks catch a reader that drifted onto field 17,
            # geospatial statistics: a struct too, but its field 1 isn't an i64.
            if last_id == SIZE_STATS_UNENCODED_BYTES:
                if field_type != I64:
                    raise ThriftDesync(
                        f"SizeStatistics field 1 has type {field_type}, expected i64"
                    )
                unencoded = self.zigzag()
            elif last_id == SIZE_STATS_REPETITION_HISTOGRAM:
                if field_type != LIST:
                    raise ThriftDesync(
                        f"SizeStatistics field 2 has type {field_type}, expected list"
                    )
                repetition = self.i64_list(last_id)
            elif last_id == SIZE_STATS_DEFINITION_HISTOGRAM:
                if field_type != LIST:
                    raise ThriftDesync(
                        f"SizeStatistics field 3 has type {field_type}, expected list"
                    )
                definition = self.i64_list(last_id)
            else:
                self.skip(field_type)
        return self.tuple_new(self.leaf_stats, (unencoded, repetition, definition))

    cdef int64_t data_pages(self) except? -1:
        cdef Py_ssize_t count, i
        cdef int element_type, field_type
        cdef int64_t last_id, page_type, entry_pages
        cdef int64_t pages = 0
        self.list_header(&count, &element_type)
        if count and element_type != STRUCT:
            raise ThriftDesync(
                f"ColumnMetaData field 13 is a list of type {element_type}, "
                "expected struct"
            )
        for i in range(count):
            # Each entry counts the pages of one type and encoding; -1 is no type.
            last_id = 0
            page_type = -1
            entry_pages = 0
            while self.next_field(&last_id, &field_type):
                if last_id == ENCODING_STATS_PAGE_TYPE and field_type == I32:
                    page_type = self.zigzag()
                elif last_id == ENCODING_STATS_COUNT and field_type == I32:
                    entry_pages = self.zigzag()
                else:
                    self.skip(field_type)
            if page_type == DATA_PAGE or page_type == DATA_PAGE_V2:
                pages += entry_pages
        return pages

    cdef object column_metadata(self, int64_t* pages):
        cdef int64_t last_id = 0
        cdef int field_type
        size_stats = None
        while self.next_field(&last_id, &field_type):
            if last_id == COLUMN_META_SIZE_STATISTICS:
                if field_type != STRUCT:
                    raise ThriftDesync(
                        f"ColumnMetaData field 16 has type {field_type}, expected struct"
                    )
                size_stats = self.size_statistics()
            elif last_id == COLUMN_META_ENCODING_STATS:
                if field_type != LIST:
                    raise ThriftDesync(
                        f"ColumnMetaData field 13 has type {field_type}, expected list"
                    )
                pages[0] = self.data_pages()
            elif is_varint_type(field_type):
                self.skip_varint()
            else:
                self.skip(field_type)
        return size_stats

    cdef object column_chunk(self, int64_t* pages):
        cdef int64_t last_id = 0
        cdef int field_type
        stats = None
        while self.next_field(&last_id, &field_type):
            if last_id == COLUMN_CHUNK_META_DATA and field_type == STRUCT:
                stats = self.column_metadata(pages)
            else:
                self.skip(field_type)
        return stats

    cdef tuple row_group(self):
        cdef int64_t last_id = 0
        cdef int field_type, element_type
        cdef Py_ssize_t count, i
        cdef int64_t pages
        cdef list stats = []
        cdef list data_pages = []
        while self.next_field(&last_id, &field_type):
            if last_id == ROW_GROUP_COLUMNS and field_type == LIST:
                self.list_header(&count, &element_type)
                for i in range(count):
                    pages = 0
                    stats.append(self.column_chunk(&pages))
                    # No data pages counted means no count to go by.
                    data_pages.append(pages if pages > 0 else None)
            else:
                self.skip(field_type)
        return stats, data_pages

    def read(self):
        """``leaf_stats`` and data page counts, each by ``[row_group][leaf]`` and
        ``None`` where a leaf has none."""
        cdef int64_t last_id = 0
        cdef int field_type, element_type
        cdef Py_ssize_t count, i
        cdef list stats = []
        cdef list data_pages = []
        cdef tuple row_group
        while self.next_field(&last_id, &field_type):
            if last_id == FILE_META_ROW_GROUPS and field_type == LIST:
                self.list_header(&count, &element_type)
                for i in range(count):
                    row_group = self.row_group()
                    stats.append(row_group[0])
                    data_pages.append(row_group[1])
            else:
                self.skip(field_type)
        return stats, data_pages

    def dictionary_page(self):
        """A dictionary page header's ``(num_values, uncompressed_page_size)``, or
        ``None`` for another page's."""
        cdef int64_t last_id = 0
        cdef int64_t sub_id
        cdef int field_type, sub_type
        page_type = uncompressed = num_values = None
        while self.next_field(&last_id, &field_type):
            if last_id == PAGE_HEADER_TYPE and field_type == I32:
                page_type = self.zigzag()
            elif last_id == PAGE_HEADER_UNCOMPRESSED_SIZE and field_type == I32:
                uncompressed = self.zigzag()
            elif last_id == PAGE_HEADER_DICTIONARY_PAGE_HEADER and field_type == STRUCT:
                sub_id = 0
                while self.next_field(&sub_id, &sub_type):
                    if sub_id == DICTIONARY_PAGE_HEADER_NUM_VALUES and sub_type == I32:
                        num_values = self.zigzag()
                    else:
                        self.skip(sub_type)
            else:
                self.skip(field_type)
        if (
            page_type != DICTIONARY_PAGE
            or uncompressed is None
            or num_values is None
            or num_values < 0
            or uncompressed < 0
        ):
            return None
        return num_values, uncompressed
