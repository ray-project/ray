# distutils: language = c++

from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string


# Minimal Arrow C++ type declarations (only what we reference in Cython).
# The actual types come from Arrow headers during C++ compilation.

cdef extern from "arrow/c/abi.h" nogil:
    cdef struct ArrowArray:
        pass
    cdef struct ArrowSchema:
        pass

cdef extern from "arrow/table.h" namespace "arrow" nogil:
    cdef cppclass CTable "arrow::Table":
        pass

cdef extern from "arrow/record_batch.h" namespace "arrow" nogil:
    cdef cppclass CRecordBatch "arrow::RecordBatch":
        pass

cdef extern from "arrow/status.h" namespace "arrow" nogil:
    cdef cppclass CStatus "arrow::Status":
        c_bool ok()

cdef extern from "arrow/result.h" namespace "arrow" nogil:
    cdef cppclass CResult "arrow::Result" [T]:
        c_bool ok()
        T ValueUnsafe()

cdef extern from "arrow/c/bridge.h" namespace "arrow" nogil:
    CStatus ExportRecordBatch "arrow::ExportRecordBatch" (
        const CRecordBatch &batch,
        ArrowArray *out_array,
        ArrowSchema *out_schema)
    CResult[shared_ptr[CRecordBatch]] ImportRecordBatch "arrow::ImportRecordBatch" (
        ArrowArray *array,
        ArrowSchema *schema)

cdef extern from "sys/types.h" nogil:
    ctypedef int pid_t

cdef extern from "ray/flight_store/arrow_flight_store.h" namespace "ray::flight_store" nogil:
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
        # IPC-based methods (pass IPC bytes as std::string).
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
