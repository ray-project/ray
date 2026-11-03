# cython: language_level=3, boundscheck=True, wraparound=False, initializedcheck=False
"""Compiled footer walk for :func:`parquet_size_statistics.read_size_statistics`.

The same walk as ``parquet_size_statistics._CompactReader.read``, about 30 times
faster. Bazel builds it into Ray's wheels; see python/ray/data/BUILD.bazel.
"""
from cpython.ref cimport Py_INCREF
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from libc.stdint cimport int64_t, uint64_t

# The constants in parquet_size_statistics.py, which say what each one means.
cdef enum:
    BOOL_TRUE = 1
    BOOL_FALSE = 2
    BYTE = 3
    I16 = 4
    I32 = 5
    I64 = 6
    DOUBLE = 7
    BINARY = 8
    LIST = 9
    SET = 10
    MAP = 11
    STRUCT = 12
    DOUBLE_BYTES = 8
    NIBBLE_BITS = 4
    NIBBLE_MASK = 0x0F
    LONG_FORM_ID = 0
    LONG_FORM_COUNT = 0x0F
    STOP = 0x00
    VARINT_BITS = 7
    VARINT_VALUE_MASK = 0x7F
    VARINT_CONTINUES = 0x80
    FILE_META_ROW_GROUPS = 4
    ROW_GROUP_COLUMNS = 1
    COLUMN_CHUNK_META_DATA = 3
    COLUMN_META_SIZE_STATISTICS = 16
    SIZE_STATS_UNENCODED_BYTES = 1
    SIZE_STATS_REPETITION_HISTOGRAM = 2
    SIZE_STATS_DEFINITION_HISTOGRAM = 3


class ThriftDesync(Exception):
    """A field's type contradicts parquet.thrift: the reader has lost its place."""


cdef inline bint is_varint_type(int t):
    return t == I16 or t == I32 or t == I64


cdef class CompactReader:
    """Reads a footer's ``SizeStatistics`` into ``leaf_stats`` tuples.

    Past the end raises IndexError, as the Python reader does.
    """

    cdef const unsigned char[::1] buf
    cdef Py_ssize_t pos
    cdef object leaf_stats
    cdef object tuple_new

    def __cinit__(self, buf, leaf_stats):
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
            c = <Py_ssize_t>self.varint()
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
            count = <Py_ssize_t>self.varint()
            self.pos += count
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
            count = <Py_ssize_t>self.varint()
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

    cdef object column_metadata(self):
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
            elif is_varint_type(field_type):
                self.skip_varint()
            else:
                self.skip(field_type)
        return size_stats

    cdef object column_chunk(self):
        cdef int64_t last_id = 0
        cdef int field_type
        stats = None
        while self.next_field(&last_id, &field_type):
            if last_id == COLUMN_CHUNK_META_DATA and field_type == STRUCT:
                stats = self.column_metadata()
            else:
                self.skip(field_type)
        return stats

    cdef list row_group(self):
        cdef int64_t last_id = 0
        cdef int field_type, element_type
        cdef Py_ssize_t count, i
        cdef list columns = []
        while self.next_field(&last_id, &field_type):
            if last_id == ROW_GROUP_COLUMNS and field_type == LIST:
                self.list_header(&count, &element_type)
                columns = [self.column_chunk() for i in range(count)]
            else:
                self.skip(field_type)
        return columns

    def read(self):
        """``leaf_stats`` by ``[row_group][leaf]``, ``None`` where a leaf has none."""
        cdef int64_t last_id = 0
        cdef int field_type, element_type
        cdef Py_ssize_t count, i
        cdef list row_groups = []
        while self.next_field(&last_id, &field_type):
            if last_id == FILE_META_ROW_GROUPS and field_type == LIST:
                self.list_header(&count, &element_type)
                row_groups = [self.row_group() for i in range(count)]
            else:
                self.skip(field_type)
        return row_groups
