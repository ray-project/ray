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

# cython: language_level = 3

from libc.stdint cimport *
from libcpp cimport bool as c_bool, nullptr
from libcpp.memory cimport shared_ptr, unique_ptr, make_shared
from libcpp.string cimport string as c_string
from libcpp.utility cimport pair
from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.unordered_set cimport unordered_set

cdef extern from "arrow/status.h" namespace "arrow" nogil:

    # We can later add more of the common status factory methods as needed
    cdef CStatus CStatus_OK "arrow::Status::OK"()
    cdef CStatus CStatus_Invalid "arrow::Status::Invalid"()
    cdef CStatus CStatus_NotImplemented \
        "arrow::Status::NotImplemented"(const c_string& msg)
    cdef CStatus CStatus_UnknownError \
        "arrow::Status::UnknownError"(const c_string& msg)

    cdef cppclass CStatus "arrow::Status":
        CStatus()

        c_string ToString()
        c_string message()

        c_bool ok()
        c_bool IsIOError()
        c_bool IsOutOfMemory()
        c_bool IsInvalid()
        c_bool IsKeyError()
        c_bool IsNotImplemented()
        c_bool IsTypeError()
        c_bool IsCapacityError()
        c_bool IsSerializationError()
        c_bool IsPythonError()
        c_bool IsPlasmaObjectExists()
        c_bool IsPlasmaObjectNonexistent()
        c_bool IsPlasmaStoreFull()

cdef extern from "arrow/buffer.h" namespace "arrow" nogil:

    cdef cppclass CBuffer" arrow::Buffer":
        CBuffer(const uint8_t* data, int64_t size)
        const uint8_t* data()
        uint8_t* mutable_data()
        int64_t size()
        shared_ptr[CBuffer] parent()
        c_bool is_mutable() const
        c_bool Equals(const CBuffer& other)

    cdef cppclass CMutableBuffer" arrow::MutableBuffer"(CBuffer):
        CMutableBuffer(const uint8_t* data, int64_t size)


cdef extern from "arrow/io/interfaces.h" namespace "arrow::io" nogil:

    enum FileMode" arrow::io::FileMode::type":
        FileMode_READ" arrow::io::FileMode::READ"
        FileMode_WRITE" arrow::io::FileMode::WRITE"
        FileMode_READWRITE" arrow::io::FileMode::READWRITE"

    cdef cppclass FileInterface:
        CStatus Close()
        CStatus Tell(int64_t* position)
        FileMode mode()
        c_bool closed()

    cdef cppclass Seekable:
        CStatus Seek(int64_t position)

    cdef cppclass Writable:
        CStatus Write(const uint8_t* data, int64_t nbytes)

    cdef cppclass OutputStream(FileInterface, Writable):
        pass

    cdef cppclass WritableFile(OutputStream, Seekable):
        CStatus WriteAt(int64_t position, const uint8_t* data,
                        int64_t nbytes)

cdef extern from "arrow/io/memory.h" namespace "arrow::io" nogil:

    cdef cppclass CFixedSizeBufferWriter \
            " arrow::io::FixedSizeBufferWriter"(WritableFile):
        CFixedSizeBufferWriter(const shared_ptr[CBuffer]& buffer)

        void set_memcopy_threads(int num_threads)
        void set_memcopy_blocksize(int64_t blocksize)
        void set_memcopy_threshold(int64_t threshold)

cdef extern from "arrow/python/common.h" namespace "arrow::py" nogil:
    cdef cppclass PyBuffer(CBuffer):
        @staticmethod
        CStatus FromPyObject(object obj, shared_ptr[CBuffer]* out)


cdef int check_status(const CStatus& status) nogil except -1

cdef class Buffer:
    cdef:
        shared_ptr[CBuffer] buffer
        Py_ssize_t shape[1]
        Py_ssize_t strides[1]

    cdef void init(self, const shared_ptr[CBuffer]& buffer)
    cdef getitem(self, int64_t i)

cdef object pyarrow_wrap_buffer(const shared_ptr[CBuffer]& buf)
