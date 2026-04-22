# distutils: language = c++

from libcpp cimport bool as c_bool
from libcpp.string cimport string as c_string


cdef extern from "sys/types.h" nogil:
    ctypedef int pid_t

# Use the lightweight API header that has no Arrow includes.
cdef extern from "ray/flight_store/arrow_flight_store_api.h" \
        namespace "ray::flight_store" nogil:
    cdef struct CObjectTransferInfo "ray::flight_store::ObjectTransferInfo":
        c_string flight_uri
        c_string key
        pid_t pid
        long long ipc_size

    cdef cppclass CArrowFlightStore "ray::flight_store::ArrowFlightStore":
        CArrowFlightStore() except +
        int StartServer() except +
        void StopServer() except +
        c_string GetUri()
        void PutFromIPC(const c_string &key, const c_string &ipc_bytes) except +
        CObjectTransferInfo PutFromIPCAndGetTransferInfo(
            const c_string &key, const c_string &ipc_bytes) except +
        c_string GetLocalAsIPC(const c_string &key) except +
        c_string FetchAsIPC(const c_string &uri, const c_string &key) except +
        c_string FetchViaVMAsIPC(
            const c_string &flight_uri, const c_string &key,
            long long ipc_size) except +
        void Delete(const c_string &key)
        size_t Size()
