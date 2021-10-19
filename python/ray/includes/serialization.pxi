from libc.string cimport memcpy
from libc.stdint cimport uintptr_t, uint64_t, INT32_MAX
import cython

DEF MEMCOPY_THREADS = 6

# This is the default alignment value for len(buffer) < 2048.
DEF kMinorBufferAlign = 8
# This is the default alignment value for len(buffer) >= 2048.
# Some projects like Arrow use it for possible SIMD acceleration.
DEF kMajorBufferAlign = 64
DEF kMajorBufferSize = 2048
DEF kMemcopyDefaultBlocksize = 64
DEF kMemcopyDefaultThreshold = 1024 * 1024
DEF kLanguageSpecificTypeExtensionId = 101
DEF kMessagePackOffset = 9

cdef extern from "ray/util/memory.h" namespace "ray" nogil:
    void parallel_memcopy(uint8_t* dst, const uint8_t* src, int64_t nbytes,
                          uintptr_t block_size, int num_threads)

cdef extern from "google/protobuf/repeated_field.h" nogil:
    cdef cppclass RepeatedField[Element]:
        const Element* data() const

cdef extern from "src/ray/protobuf/serialization.pb.h" nogil:
    cdef cppclass CPythonBuffer "ray::serialization::PythonBuffer":
        void set_address(uint64_t value)
        uint64_t address() const
        void set_length(int64_t value)
        int64_t length() const
        void set_itemsize(int64_t value)
        int64_t itemsize()
        void set_ndim(int32_t value)
        int32_t ndim()
        void set_readonly(c_bool value)
        c_bool readonly()
        void set_format(const c_string& value)
        const c_string &format()
        c_string* release_format()
        void add_shape(int64_t value)
        int64_t shape(int index)
        const RepeatedField[int64_t] &shape() const
        int shape_size()
        void add_strides(int64_t value)
        int64_t strides(int index)
        const RepeatedField[int64_t] &strides() const
        int strides_size()

    cdef cppclass CPythonObject "ray::serialization::PythonObject":
        uint64_t inband_data_size() const
        void set_inband_data_size(uint64_t value)
        uint64_t raw_buffers_size() const
        void set_raw_buffers_size(uint64_t value)
        CPythonBuffer* add_buffer()
        CPythonBuffer& buffer(int index) const
        int buffer_size() const
        size_t ByteSizeLong() const
        int GetCachedSize() const
        uint8_t *SerializeWithCachedSizesToArray(uint8_t *target)
        c_bool ParseFromArray(void* data, int size)


cdef int64_t padded_length(int64_t offset, int64_t alignment):
    return ((offset + alignment - 1) // alignment) * alignment


cdef uint8_t* aligned_address(uint8_t* addr, uint64_t alignment) nogil:
    cdef uintptr_t u_addr = <uintptr_t>addr
    return <uint8_t*>(((u_addr + alignment - 1) // alignment) * alignment)


cdef class SubBuffer:
    cdef:
        void *buf
        Py_ssize_t len
        int readonly
        c_string _format
        int ndim
        c_vector[Py_ssize_t] _shape
        c_vector[Py_ssize_t] _strides
        Py_ssize_t *suboffsets
        Py_ssize_t itemsize
        void *internal
        object buffer

    def __cinit__(self, object buffer):
        # Increase ref count.
        self.buffer = buffer
        self.suboffsets = NULL
        self.internal = NULL

    def __len__(self):
        return self.len // self.itemsize

    @property
    def nbytes(self):
        """
        The buffer size in bytes.
        """
        return self.len

    @property
    def readonly(self):
        return self.readonly

    def tobytes(self):
        """
        Return this buffer as a Python bytes object. Memory is copied.
        """
        return PyBytes_FromStringAndSize(
            <const char*> self.buf, self.len)

    def __getbuffer__(self, Py_buffer* buffer, int flags):
        if flags & cpython.PyBUF_WRITABLE:
            # Ray ensures all buffers are immutable.
            raise BufferError
        buffer.readonly = self.readonly
        buffer.buf = self.buf
        buffer.format = <char *>self._format.c_str()
        buffer.internal = self.internal
        buffer.itemsize = self.itemsize
        buffer.len = self.len
        buffer.ndim = self.ndim
        buffer.obj = self  # This is important for GC.
        buffer.shape = self._shape.data()
        buffer.strides = self._strides.data()
        buffer.suboffsets = self.suboffsets

    def __getsegcount__(self, Py_ssize_t *len_out):
        if len_out != NULL:
            len_out[0] = <Py_ssize_t> self.size
        return 1

    def __getreadbuffer__(self, Py_ssize_t idx, void ** p):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = self.buf
        return self.size

    def __getwritebuffer__(self, Py_ssize_t idx, void ** p):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = self.buf
        return self.size


cdef class MessagePackSerializer(object):
    @staticmethod
    def dumps(o, python_serializer=None):
        def _default(obj):
            if python_serializer is not None:
                return msgpack.ExtType(kLanguageSpecificTypeExtensionId,
                                       msgpack.dumps(python_serializer(obj)))
            return obj
        try:
            # If we let strict_types is False, then whether list or tuple will
            # be packed to a message pack array. So, they can't be
            # distinguished when unpacking.
            return msgpack.dumps(o, default=_default,
                                 use_bin_type=True, strict_types=True)
        except ValueError as ex:
            # msgpack can't handle recursive objects, so we serialize them by
            # python serializer, e.g. pickle.
            return msgpack.dumps(_default(o), default=_default,
                                 use_bin_type=True, strict_types=True)

    @classmethod
    def loads(cls, s, python_deserializer=None):
        def _ext_hook(code, data):
            if code == kLanguageSpecificTypeExtensionId:
                if python_deserializer is not None:
                    return python_deserializer(msgpack.loads(data))
                raise Exception('Unrecognized ext type id: {}'.format(code))
        try:
            gc.disable()  # Performance optimization for msgpack.
            return msgpack.loads(s, ext_hook=_ext_hook, raw=False,
                                 strict_map_key=False)
        finally:
            gc.enable()


@cython.boundscheck(False)
@cython.wraparound(False)
def split_buffer(Buffer buf):
    cdef:
        const uint8_t *data = buf.buffer.get().Data()
        size_t size = buf.buffer.get().Size()
        uint8_t[:] bufferview = buf
        int64_t msgpack_bytes_length

    assert kMessagePackOffset <= size
    header_unpacker = msgpack.Unpacker()
    header_unpacker.feed(bufferview[:kMessagePackOffset])
    msgpack_bytes_length = header_unpacker.unpack()
    assert kMessagePackOffset + msgpack_bytes_length <= <int64_t>size
    return (bufferview[kMessagePackOffset:
                       kMessagePackOffset + msgpack_bytes_length],
            bufferview[kMessagePackOffset + msgpack_bytes_length:])


# Note [Pickle5 serialization layout & alignment]
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# To ensure efficient data access, our serialize enforces alignment
# when writing data to a buffer. See 'serialization.proto' for
# the detail memory layout and alignment.


@cython.boundscheck(False)
@cython.wraparound(False)
def unpack_pickle5_buffers(uint8_t[:] bufferview):
    cdef:
        const uint8_t *data = &bufferview[0]
        CPythonObject python_object
        CPythonBuffer *buffer_meta
        int inband_offset = sizeof(int64_t) * 2
        int64_t inband_size
        int64_t protobuf_size
        int32_t i
        const uint8_t *buffers_segment
    inband_size = (<int64_t*>data)[0]
    if inband_size < 0:
        raise ValueError("The inband data size should be positive."
                         "Got negative instead. "
                         "Maybe the buffer has been corrupted.")
    protobuf_size = (<int64_t*>data)[1]
    if protobuf_size > INT32_MAX or protobuf_size < 0:
        raise ValueError("Incorrect protobuf size. "
                         "Maybe the buffer has been corrupted.")
    inband_data = bufferview[inband_offset:inband_offset + inband_size]
    if not python_object.ParseFromArray(
            data + inband_offset + inband_size, <int32_t>protobuf_size):
        raise ValueError("Protobuf object is corrupted.")
    buffers_segment = aligned_address(
        <uint8_t*>data + inband_offset + inband_size + protobuf_size,
        kMajorBufferAlign)
    pickled_buffers = []
    # Now read buffer meta
    for i in range(python_object.buffer_size()):
        buffer_meta = <CPythonBuffer *>&python_object.buffer(i)
        buffer = SubBuffer(bufferview)
        buffer.buf = <void*>(buffers_segment + buffer_meta.address())
        buffer.len = buffer_meta.length()
        buffer.itemsize = buffer_meta.itemsize()
        buffer.readonly = buffer_meta.readonly()
        buffer.ndim = buffer_meta.ndim()
        buffer._format = buffer_meta.format()
        buffer._shape.assign(
          buffer_meta.shape().data(),
          buffer_meta.shape().data() + buffer_meta.ndim())
        buffer._strides.assign(
          buffer_meta.strides().data(),
          buffer_meta.strides().data() + buffer_meta.ndim())
        buffer.internal = NULL
        buffer.suboffsets = NULL
        pickled_buffers.append(buffer)
    return inband_data, pickled_buffers


cdef class Pickle5Writer:
    cdef:
        CPythonObject python_object
        c_vector[Py_buffer] buffers
        # Address of end of the current buffer, relative to the
        # begin offset of our buffers.
        uint64_t _curr_buffer_addr
        uint64_t _protobuf_offset
        int64_t _total_bytes

    def __cinit__(self):
        self._curr_buffer_addr = 0
        self._total_bytes = -1

    def __dealloc__(self):
        # We must release the buffer, or we could experience memory leaks.
        for i in range(self.buffers.size()):
            cpython.PyBuffer_Release(&self.buffers[i])

    def buffer_callback(self, pickle_buffer):
        cdef:
            Py_buffer view
            int32_t i
            CPythonBuffer* buffer = self.python_object.add_buffer()
        cpython.PyObject_GetBuffer(pickle_buffer, &view,
                                   cpython.PyBUF_FULL_RO)
        buffer.set_length(view.len)
        buffer.set_ndim(view.ndim)
        # It should be 'view.readonly'. But for the sake of shared memory,
        # we have to make it immutable.
        buffer.set_readonly(1)
        buffer.set_itemsize(view.itemsize)
        if view.format:
            buffer.set_format(view.format)
        if view.shape:
            for i in range(view.ndim):
                buffer.add_shape(view.shape[i])
        if view.strides:
            for i in range(view.ndim):
                buffer.add_strides(view.strides[i])

        # Increase buffer address.
        if view.len < kMajorBufferSize:
            self._curr_buffer_addr = padded_length(
                self._curr_buffer_addr, kMinorBufferAlign)
        else:
            self._curr_buffer_addr = padded_length(
                self._curr_buffer_addr, kMajorBufferAlign)
        buffer.set_address(self._curr_buffer_addr)
        self._curr_buffer_addr += view.len
        self.buffers.push_back(view)

    def get_total_bytes(self, const uint8_t[:] inband):
        cdef:
            size_t protobuf_bytes = 0
            uint64_t inband_data_offset = sizeof(int64_t) * 2
        self.python_object.set_inband_data_size(len(inband))
        self.python_object.set_raw_buffers_size(self._curr_buffer_addr)
        # Since calculating the output size is expensive, we will
        # reuse the cached size.
        # However, protobuf could change the output size according to
        # different values, so we MUST NOT change 'python_object' afterwards.
        protobuf_bytes = self.python_object.ByteSizeLong()
        if protobuf_bytes > INT32_MAX:
            raise ValueError("Total buffer metadata size is bigger than %d. "
                             "Consider reduce the number of buffers "
                             "(number of numpy arrays, etc)." % INT32_MAX)
        self._protobuf_offset = inband_data_offset + len(inband)
        self._total_bytes = self._protobuf_offset + protobuf_bytes
        if self._curr_buffer_addr > 0:
            # reserve 'kMajorBufferAlign' bytes for possible buffer alignment
            self._total_bytes += kMajorBufferAlign + self._curr_buffer_addr
        return self._total_bytes

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef void write_to(self, const uint8_t[:] inband, uint8_t[:] data,
                       int memcopy_threads) nogil:
        cdef:
            uint8_t *ptr = &data[0]
            uint64_t buffer_addr
            uint64_t buffer_len
            int i
            int64_t protobuf_size = self.python_object.GetCachedSize()
        if self._total_bytes < 0:
            raise ValueError("Must call 'get_total_bytes()' first "
                             "to get the actual size")
        # Write inband data & protobuf size for deserialization.
        (<int64_t*>ptr)[0] = len(inband)
        (<int64_t*>ptr)[1] = protobuf_size
        # Write inband data.
        ptr += sizeof(int64_t) * 2
        memcpy(ptr, &inband[0], len(inband))
        # Write protobuf data.
        ptr += len(inband)
        self.python_object.SerializeWithCachedSizesToArray(ptr)
        ptr += protobuf_size
        if self._curr_buffer_addr <= 0:
            # End of serialization. Writing more stuff will corrupt the memory.
            return
        # aligned to 64 bytes
        ptr = aligned_address(ptr, kMajorBufferAlign)
        for i in range(self.python_object.buffer_size()):
            buffer_addr = self.python_object.buffer(i).address()
            buffer_len = self.python_object.buffer(i).length()
            if (memcopy_threads > 1 and
                    buffer_len > kMemcopyDefaultThreshold):
                parallel_memcopy(ptr + buffer_addr,
                                 <const uint8_t*> self.buffers[i].buf,
                                 buffer_len,
                                 kMemcopyDefaultBlocksize, memcopy_threads)
            else:
                memcpy(ptr + buffer_addr, self.buffers[i].buf, buffer_len)


cdef class SerializedObject(object):
    cdef:
        object _metadata
        object _contained_object_refs

    def __init__(self, metadata, contained_object_refs=None):
        self._metadata = metadata
        self._contained_object_refs = contained_object_refs or []

    @property
    def total_bytes(self):
        raise NotImplementedError("{}.total_bytes not implemented.".format(
                type(self).__name__))

    @property
    def metadata(self):
        return self._metadata

    @property
    def contained_object_refs(self):
        return self._contained_object_refs

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef void write_to(self, uint8_t[:] buffer) nogil:
        raise NotImplementedError("{}.write_to not implemented.".format(
                type(self).__name__))


cdef class Pickle5SerializedObject(SerializedObject):
    cdef:
        const uint8_t[:] inband
        Pickle5Writer writer
        object _total_bytes

    def __init__(self, metadata, inband, Pickle5Writer writer,
                 contained_object_refs):
        super(Pickle5SerializedObject, self).__init__(metadata,
                                                      contained_object_refs)
        self.inband = inband
        self.writer = writer
        # cached total bytes
        self._total_bytes = None

    @property
    def total_bytes(self):
        if self._total_bytes is None:
            self._total_bytes = self.writer.get_total_bytes(self.inband)
        return self._total_bytes

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef void write_to(self, uint8_t[:] buffer) nogil:
        self.writer.write_to(self.inband, buffer, MEMCOPY_THREADS)


cdef class MessagePackSerializedObject(SerializedObject):
    cdef:
        SerializedObject nest_serialized_object
        object msgpack_header
        object msgpack_data
        int64_t _msgpack_header_bytes
        int64_t _msgpack_data_bytes
        int64_t _total_bytes
        const uint8_t *msgpack_header_ptr
        const uint8_t *msgpack_data_ptr

    def __init__(self, metadata, msgpack_data, contained_object_refs,
                 SerializedObject nest_serialized_object=None):
        if nest_serialized_object:
            contained_object_refs.extend(
                nest_serialized_object.contained_object_refs
            )
            total_bytes = nest_serialized_object.total_bytes
        else:
            total_bytes = 0
        super(MessagePackSerializedObject, self).__init__(
            metadata,
            contained_object_refs,
        )
        self.nest_serialized_object = nest_serialized_object
        self.msgpack_header = msgpack_header = msgpack.dumps(len(msgpack_data))
        self.msgpack_data = msgpack_data
        self._msgpack_header_bytes = len(msgpack_header)
        self._msgpack_data_bytes = len(msgpack_data)
        self._total_bytes = (kMessagePackOffset +
                             self._msgpack_data_bytes +
                             total_bytes)
        self.msgpack_header_ptr = <const uint8_t*>msgpack_header
        self.msgpack_data_ptr = <const uint8_t*>msgpack_data
        assert self._msgpack_header_bytes <= kMessagePackOffset

    @property
    def total_bytes(self):
        return self._total_bytes

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef void write_to(self, uint8_t[:] buffer) nogil:
        cdef uint8_t *ptr = &buffer[0]

        # Write msgpack data first.
        memcpy(ptr, self.msgpack_header_ptr, self._msgpack_header_bytes)
        memcpy(ptr + kMessagePackOffset,
               self.msgpack_data_ptr, self._msgpack_data_bytes)

        if self.nest_serialized_object is not None:
            self.nest_serialized_object.write_to(
                buffer[kMessagePackOffset + self._msgpack_data_bytes:])


cdef class RawSerializedObject(SerializedObject):
    cdef:
        object value
        const uint8_t *value_ptr
        int64_t _total_bytes

    def __init__(self, value):
        super(RawSerializedObject,
              self).__init__(ray_constants.OBJECT_METADATA_TYPE_RAW)
        self.value = value
        self.value_ptr = <const uint8_t*> value
        self._total_bytes = len(value)

    @property
    def total_bytes(self):
        return self._total_bytes

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef void write_to(self, uint8_t[:] buffer) nogil:
        if (MEMCOPY_THREADS > 1 and
                self._total_bytes > kMemcopyDefaultThreshold):
            parallel_memcopy(&buffer[0],
                             self.value_ptr,
                             self._total_bytes, kMemcopyDefaultBlocksize,
                             MEMCOPY_THREADS)
        else:
            memcpy(&buffer[0], self.value_ptr, self._total_bytes)
