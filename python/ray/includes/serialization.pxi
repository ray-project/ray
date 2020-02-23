from libc.stdint cimport uintptr_t, uint64_t, INT32_MAX

# NOTE: according to Python standard, Py_ssize_t is just ssize_t. We will use ssize_t in the C++ side.

cdef extern from "ray/python/buffer_writer.h" nogil:
    cdef cppclass CPythonObjectBuilder "ray::python::PythonObjectBuilder":
        CPythonObjectBuilder()
        void AppendBuffer(uint8_t *buf, ssize_t length, ssize_t itemsize, int32_t ndim,
                          char* format, ssize_t *shape, ssize_t *strides)
        int64_t GetTotalBytes()
        int64_t GetInbandDataSize()
        void SetInbandDataSize(int64_t size)
        int WriteTo(const c_string &inband, const shared_ptr[CBuffer] &data, int memcopy_threads)

    int CRawDataWrite "ray::python::RawDataWrite" (const c_string &raw_data,
                      const shared_ptr[CBuffer] &data,
                      int memcopy_threads)


cdef extern from "ray/python/buffer_reader.h" nogil:
    cdef cppclass CPythonObjectParser "ray::python::PythonObjectParser":
        PythonObjectParser()
        int Parse(const shared_ptr[CBuffer] &buffer)
        c_string GetInbandData()
        int64_t BuffersCount()
        void GetBuffer(int index, void **buf, ssize_t *length, int *readonly, ssize_t *itemsize, int *ndim,
                       c_string *format, c_vector[ssize_t] *shape, c_vector[ssize_t] *strides)


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
        c_string format
        int ndim
        c_vector[Py_ssize_t] shape
        c_vector[Py_ssize_t] strides
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
        buffer.format = <char*>self.format.c_str()
        buffer.internal = self.internal
        buffer.itemsize = self.itemsize
        buffer.len = self.len
        buffer.ndim = self.ndim
        buffer.obj = self  # This is important for GC.
        buffer.shape = self.shape.data()
        buffer.strides = self.strides.data()
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
        CPythonObjectParser parser
        int32_t i
        int status

    status = parser.Parse(buf.buffer)
    if status != 0:
        if status == -1:
            raise ValueError("Incorrect protobuf data offset. "
                             "Maybe the buffer has been corrupted.")
        elif status == -2:
            raise ValueError("Incorrect protobuf size. "
                             "Maybe the buffer has been corrupted.")
        elif status == -3:
            raise ValueError("Protobuf object is corrupted.")
        else:
            raise ValueError("Parsing the serialized python object "
                             "failed with unknown status %d" % status)
    # Now read buffers
    pickled_buffers = []
    for i in range(parser.BuffersCount()):
        buffer = SubBuffer(buf)
        parser.GetBuffer(i, &buffer.buf, &buffer.len, &buffer.readonly, &buffer.itemsize, &buffer.ndim,
                         &buffer.format, &buffer.shape, &buffer.strides)
        buffer.internal = NULL
        buffer.suboffsets = NULL
        pickled_buffers.append(buffer)
    return parser.GetInbandData(), pickled_buffers


cdef class Pickle5Writer:
    cdef:
        CPythonObjectBuilder builder
        c_vector[Py_buffer] buffers

    def __cinit__(self):
        pass

    def buffer_callback(self, pickle_buffer):
        cdef Py_buffer view
        cpython.PyObject_GetBuffer(pickle_buffer, &view,
                                   cpython.PyBUF_FULL_RO)
        self.builder.AppendBuffer(<uint8_t *>view.buf, view.len, view.itemsize, view.ndim, view.format,
                                  view.shape, view.strides)
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
