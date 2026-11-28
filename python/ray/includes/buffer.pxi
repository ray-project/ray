from cpython cimport Py_buffer, PyBytes_FromStringAndSize
from libc.stdint cimport int64_t, uintptr_t
from libc.stdio cimport printf
from libcpp.memory cimport shared_ptr

from ray.includes.common cimport CBuffer


cdef class Buffer:
    """Cython wrapper class of C++ `ray::Buffer`.

    This class implements the Python 'buffer protocol', which allows
    us to use it for calls into Python libraries without having to
    copy the data.

    See https://docs.python.org/3/c-api/buffer.html for details.
    """
    @staticmethod
    cdef make(const shared_ptr[CBuffer]& buffer):
        cdef Buffer self = Buffer.__new__(Buffer)
        self.buffer = buffer
        self.shape = <Py_ssize_t>self.size
        self.strides = <Py_ssize_t>(1)
        return self

    def __len__(self):
        return self.size

    @property
    def size(self):
        """
        The buffer size in bytes.
        """
        return self.buffer.get().Size()

    def to_pybytes(self):
        """
        Return this buffer as a Python bytes object.  Memory is copied.
        """
        return PyBytes_FromStringAndSize(
            <const char*>self.buffer.get().Data(),
            self.buffer.get().Size())

    def __getbuffer__(self, Py_buffer* buffer, int flags):
        buffer.readonly = 0
        buffer.buf = <char *>self.buffer.get().Data()
        buffer.format = 'B'
        buffer.internal = NULL
        buffer.itemsize = 1
        buffer.len = self.size
        buffer.ndim = 1
        buffer.obj = self
        buffer.shape = &self.shape
        buffer.strides = &self.strides
        buffer.suboffsets = NULL
