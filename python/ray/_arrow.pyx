# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import socket
import sys

from _arrow cimport CStatus
# from pyarrow.includes.common cimport c_string
from pyarrow.compat import frombytes
cimport cpython as cp


class ArrowException(Exception):
    pass


class ArrowInvalid(ValueError, ArrowException):
    pass


class ArrowMemoryError(MemoryError, ArrowException):
    pass


class ArrowIOError(IOError, ArrowException):
    pass


class ArrowKeyError(KeyError, ArrowException):
    pass


class ArrowTypeError(TypeError, ArrowException):
    pass


class ArrowNotImplementedError(NotImplementedError, ArrowException):
    pass


class ArrowCapacityError(ArrowException):
    pass


class PlasmaObjectExists(ArrowException):
    pass


class PlasmaObjectNonexistent(ArrowException):
    pass


class PlasmaStoreFull(ArrowException):
    pass


class ArrowSerializationError(ArrowException):
    pass


cdef int check_status(const CStatus& status) nogil except -1:
    if status.ok():
        return 0

    if status.IsPythonError():
        return -1

    with gil:
        message = frombytes(status.message())
        if status.IsInvalid():
            raise ArrowInvalid(message)
        elif status.IsIOError():
            raise ArrowIOError(message)
        elif status.IsOutOfMemory():
            raise ArrowMemoryError(message)
        elif status.IsKeyError():
            raise ArrowKeyError(message)
        elif status.IsNotImplemented():
            raise ArrowNotImplementedError(message)
        elif status.IsTypeError():
            raise ArrowTypeError(message)
        elif status.IsCapacityError():
            raise ArrowCapacityError(message)
        elif status.IsPlasmaObjectExists():
            raise PlasmaObjectExists(message)
        elif status.IsPlasmaObjectNonexistent():
            raise PlasmaObjectNonexistent(message)
        elif status.IsPlasmaStoreFull():
            raise PlasmaStoreFull(message)
        elif status.IsSerializationError():
            raise ArrowSerializationError(message)
        else:
            message = frombytes(status.ToString())
            raise ArrowException(message)

cdef class Buffer:
    """
    The base class for all Arrow buffers.

    A buffer represents a contiguous memory area.  Many buffers will own
    their memory, though not all of them do.
    """

    def __cinit__(self):
        pass

    def __init__(self):
        raise TypeError("Do not call Buffer's constructor directly, use "
                        "`pyarrow.py_buffer` function instead.")

    cdef void init(self, const shared_ptr[CBuffer]& buffer):
        self.buffer = buffer
        self.shape[0] = self.size
        self.strides[0] = <Py_ssize_t>(1)

    def __len__(self):
        return self.size

    @property
    def size(self):
        """
        The buffer size in bytes.
        """
        return self.buffer.get().size()

    @property
    def address(self):
        """
        The buffer's address, as an integer.
        """
        return <uintptr_t> self.buffer.get().data()

    @property
    def is_mutable(self):
        """
        Whether the buffer is mutable.
        """
        return self.buffer.get().is_mutable()

    cdef getitem(self, int64_t i):
        return self.buffer.get().data()[i]

    def __getbuffer__(self, cp.Py_buffer* buffer, int flags):
        if self.buffer.get().is_mutable():
            buffer.readonly = 0
        else:
            if flags & cp.PyBUF_WRITABLE:
                raise BufferError("Writable buffer requested but Arrow "
                                  "buffer was not mutable")
            buffer.readonly = 1
        buffer.buf = <char *>self.buffer.get().data()
        buffer.format = 'b'
        buffer.internal = NULL
        buffer.itemsize = 1
        buffer.len = self.size
        buffer.ndim = 1
        buffer.obj = self
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL

    def __getsegcount__(self, Py_ssize_t *len_out):
        if len_out != NULL:
            len_out[0] = <Py_ssize_t>self.size
        return 1

    def __getreadbuffer__(self, Py_ssize_t idx, void **p):
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = <void*> self.buffer.get().data()
        return self.size

    def __getwritebuffer__(self, Py_ssize_t idx, void **p):
        if not self.buffer.get().is_mutable():
            raise SystemError("trying to write an immutable buffer")
        if idx != 0:
            raise SystemError("accessing non-existent buffer segment")
        if p != NULL:
            p[0] = <void*> self.buffer.get().data()
        return self.size


cdef api object pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buf):
    cdef Buffer result = Buffer.__new__(Buffer)
    result.init(buf)
    return result

def py_buffer(object obj):
    """
    Construct an Arrow buffer from a Python bytes-like or buffer-like object
    """
    cdef shared_ptr[CBuffer] buf
    check_status(PyBuffer.FromPyObject(obj, &buf))
    return pyarrow_wrap_buffer(buf)

PY2 = sys.version_info[0] == 2

def get_socket_from_fd(fileno, family, type):
    if PY2:
        socket_obj = socket.fromfd(fileno, family, type)
        return socket.socket(family, type, _sock=socket_obj)
    else:
        return socket.socket(fileno=fileno, family=family, type=type)

cdef class FixedSizeBufferWriter:
    """
    A stream writing to a Arrow buffer.
    """

    cdef:
        shared_ptr[OutputStream] output_stream
        object is_writable

    def __cinit__(self, Buffer buffer):
        self.output_stream.reset(new CFixedSizeBufferWriter(buffer.buffer))
        self.is_writable = True

    def set_memcopy_threads(self, int num_threads):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.output_stream.get()
        writer.set_memcopy_threads(num_threads)

    def set_memcopy_blocksize(self, int64_t blocksize):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.output_stream.get()
        writer.set_memcopy_blocksize(blocksize)

    def set_memcopy_threshold(self, int64_t threshold):
        cdef CFixedSizeBufferWriter* writer = \
            <CFixedSizeBufferWriter*> self.output_stream.get()
        writer.set_memcopy_threshold(threshold)

    cdef shared_ptr[OutputStream] get_output_stream(self) except *:
        return self.output_stream

    @property
    def closed(self):
        return self.output_stream.get().closed()

    def close(self):
        if not self.closed:
            with nogil:
                check_status(self.output_stream.get().Close())

    def write(self, data):
        """
        Write byte from any object implementing buffer protocol (bytes,
        bytearray, ndarray, pyarrow.Buffer)

        Parameters
        ----------
        data : bytes-like object or exporter of buffer protocol

        Returns
        -------
        nbytes : number of bytes written
        """
        cdef Buffer arrow_buffer = py_buffer(data)

        cdef const uint8_t* buf = arrow_buffer.buffer.get().data()
        cdef int64_t bufsize = len(arrow_buffer)
        handle = self.get_output_stream()

        with nogil:
            check_status(handle.get().Write(buf, bufsize))
        return bufsize
