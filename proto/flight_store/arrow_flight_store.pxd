# distutils: language = c++

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr, unique_ptr
from libcpp.string cimport string as c_string


cdef extern from "arrow/api.h" namespace "arrow" nogil:
    cdef cppclass CTable "arrow::Table":
        pass


cdef extern from "arrow/result.h" namespace "arrow" nogil:
    cdef cppclass CResult "arrow::Result" [T]:
        c_bool ok()
        T ValueUnsafe()
        # status() omitted for brevity


cdef extern from "sys/types.h" nogil:
    ctypedef int pid_t


cdef extern from "arrow_flight_store.h" namespace "ray::flight_store" nogil:
    cdef struct CObjectTransferInfo "ray::flight_store::ObjectTransferInfo":
        c_string flight_uri
        c_string key
        pid_t pid
        long long ipc_size  # int64_t

    cdef cppclass CArrowFlightStore "ray::flight_store::ArrowFlightStore":
        CArrowFlightStore() except +
        int StartServer() except +
        void StopServer() except +
        c_string GetUri()
        void Put(const c_string &key, shared_ptr[CTable] table) except +
        CObjectTransferInfo PutAndGetTransferInfo(
            const c_string &key, shared_ptr[CTable] table) except +
        shared_ptr[CTable] GetLocal(const c_string &key)
        CResult[shared_ptr[CTable]] Fetch(
            const c_string &uri, const c_string &key) except +
        CResult[shared_ptr[CTable]] FetchViaVM(
            const c_string &flight_uri, const c_string &key,
            long long ipc_size) except +
        void Delete(const c_string &key)
        size_t Size()
