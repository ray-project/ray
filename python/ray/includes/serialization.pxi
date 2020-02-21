from libc.string cimport memcpy
from libc.stdint cimport uintptr_t, uint64_t, INT32_MAX

# This is the default alignment value for len(buffer) < 2048.
DEF kMinorBufferAlign = 8
# This is the default alignment value for len(buffer) >= 2048.
# Some projects like Arrow use it for possible SIMD acceleration.
DEF kMajorBufferAlign = 64
DEF kMajorBufferSize = 2048
DEF kMemcopyDefaultBlocksize = 64
DEF kMemcopyDefaultThreshold = 1024 * 1024

cdef extern from "ray/python/buffer_writer.h" nogil:
    cdef cppclass CPythonObjectBuilder "ray::python::PythonObjectBuilder":
        CPythonObjectBuilder()
        void AppendBuffer(uint8_t *buf, int64_t length, int64_t itemsize, int32_t ndim,
                          char* format, int64_t *shape, int64_t *strides)
        int64_t GetTotalBytes()
        int64_t GetInbandDataSize()
        void SetInbandDataSize(int64_t size)
        int WriteTo(const c_string &inband, const shared_ptr[CBuffer] &data, int memcopy_threads)

    int CRawDataWrite "ray::python::RawDataWrite" (const c_string &raw_data,
                      const shared_ptr[CBuffer] &data,
                      int memcopy_threads)


cdef extern from "google/protobuf/repeated_field.h" nogil:
    cdef cppclass RepeatedField[Element]:
        const Element* data() const

cdef extern from "ray/protobuf/serialization.pb.h" nogil:
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
        uint64_t inband_data_offset() const
        void set_inband_data_offset(uint64_t value)
        uint64_t inband_data_size() const
        void set_inband_data_size(uint64_t value)
        uint64_t raw_buffers_offset() const
        void set_raw_buffers_offset(uint64_t value)
        uint64_t raw_buffers_size() const
        void set_raw_buffers_size(uint64_t value)
        CPythonBuffer* add_buffer()
        CPythonBuffer& buffer(int index) const
        int buffer_size() const
        size_t ByteSizeLong() const
        int GetCachedSize() const
        uint8_t *SerializeWithCachedSizesToArray(uint8_t *target)
        c_bool ParseFromArray(void* data, int size)


cdef RawDataWrite(const c_string &raw_data,
                  const shared_ptr[CBuffer] &data,
                  int memcopy_threads):
    status = CRawDataWrite(raw_data, data, memcopy_threads)
    if status != 0:
        if status == -1:
            raise ValueError("The size of output buffer is insufficient. "
                             "(buffer size: %d, expected minimal size: %d)" %
                             (data.get().Size(), raw_data.length()))
        else:
            raise ValueError("Writing to the buffer failed with unknown status %d" % status)


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

    def __cinit__(self, Buffer buffer):
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


# See 'serialization.proto' for the memory layout in the Plasma buffer.
def unpack_pickle5_buffers(Buffer buf):
    cdef:
        shared_ptr[CBuffer] _buffer = buf.buffer
        const uint8_t *data = buf.buffer.get().Data()
        size_t size = _buffer.get().Size()
        CPythonObject python_object
        CPythonBuffer *buffer_meta
        c_string inband_data
        int64_t protobuf_offset
        int64_t protobuf_size
        int32_t i
        const uint8_t *buffers_segment
    protobuf_offset = (<int64_t*>data)[0]
    if protobuf_offset < 0:
        raise ValueError("The protobuf data offset should be positive."
                         "Got negative instead. "
                         "Maybe the buffer has been corrupted.")
    protobuf_size = (<int64_t*>data)[1]
    if protobuf_size > INT32_MAX or protobuf_size < 0:
        raise ValueError("Incorrect protobuf size. "
                         "Maybe the buffer has been corrupted.")
    if not python_object.ParseFromArray(
            data + protobuf_offset, <int32_t>protobuf_size):
        raise ValueError("Protobuf object is corrupted.")
    inband_data.append(<char*>(data + python_object.inband_data_offset()),
                       <size_t>python_object.inband_data_size())
    buffers_segment = data + python_object.raw_buffers_offset()
    pickled_buffers = []
    # Now read buffer meta
    for i in range(python_object.buffer_size()):
        buffer_meta = <CPythonBuffer *>&python_object.buffer(i)
        buffer = SubBuffer(buf)
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
        CPythonObjectBuilder builder
        c_vector[Py_buffer] buffers

    def __cinit__(self):
        # TODO(suquark): 32-bit OS support
        if sizeof(Py_ssize_t) != sizeof(int64_t):
            raise ValueError("Py_ssize_t isn't equal to int64_t")

    def buffer_callback(self, pickle_buffer):
        cdef Py_buffer view
        cpython.PyObject_GetBuffer(pickle_buffer, &view,
                                   cpython.PyBUF_FULL_RO)
        self.builder.AppendBuffer(<uint8_t *>view.buf, view.len, view.itemsize, view.ndim, view.format,
                                  <int64_t*>view.shape, <int64_t*>view.strides)
        self.buffers.push_back(view)

    def get_total_bytes(self):
        return self.builder.GetTotalBytes()

    def set_inband_data_size(self, size):
        self.builder.SetInbandDataSize(size)

    cdef void write_to(self, const c_string &inband, shared_ptr[CBuffer] data,
                       int memcopy_threads):
        cdef:
            int i
            int64_t total_bytes
            int status

        if self.builder.GetInbandDataSize() < 0:
            self.builder.SetInbandDataSize(inband.length())

        status = self.builder.WriteTo(inband, data, memcopy_threads)
        if status != 0:
            if status == -1:
                total_bytes = self.builder.GetTotalBytes()
                raise ValueError("The size of output buffer is insufficient. "
                                 "(buffer size: %d, expected minimal size: %d)" %
                                 (data.get().Size(), total_bytes))
            elif status == -2:
                raise ValueError("Total buffer metadata size is bigger than %d. "
                                 "Consider reduce the number of buffers in the python"
                                 "object (number of numpy arrays, etc)." % INT32_MAX)
            else:
                raise ValueError("Writing to the buffer failed with unknown status %d" % status)

        for i in range(self.buffers.size()):
            # We must release the buffer, or we could experience memory leaks.
            cpython.PyBuffer_Release(&self.buffers[i])
