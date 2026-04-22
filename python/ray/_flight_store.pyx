# cython: language_level = 3
# distutils: language = c++

from ray.includes.flight_store cimport (
    CArrowFlightStore, CTable, CObjectTransferInfo,
    ArrowArray, ArrowSchema,
    CRecordBatch, ExportRecordBatch, ImportRecordBatch, CResult, CStatus,
)
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from libc.stdint cimport uintptr_t

import pyarrow as pa


cdef class PyArrowFlightStore:
    """Python wrapper around the C++ ArrowFlightStore."""
    cdef CArrowFlightStore *_store

    def __cinit__(self):
        self._store = new CArrowFlightStore()

    def __dealloc__(self):
        cdef CArrowFlightStore *p = self._store
        self._store = NULL
        if p != NULL:
            p.StopServer()

    def start_server(self) -> int:
        return self._store.StartServer()

    def stop_server(self):
        self._store.StopServer()

    def get_uri(self) -> str:
        return self._store.GetUri().decode("utf-8")

    def put(self, str key, table):
        """Store a PyArrow Table by key.

        Converts to IPC bytes in Python, passes bytes to C++ which
        deserializes to arrow::Table. One copy, but avoids Cython template issues.
        """
        if not isinstance(table, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        # Serialize table to IPC bytes in Python.
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        cdef bytes ipc_bytes = sink.getvalue().to_pybytes()
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string c_ipc = ipc_bytes
        self._store.PutFromIPC(c_key, c_ipc)

    def put_and_get_transfer_info(self, str key, table):
        """Store a table and return transfer info."""
        if not isinstance(table, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        cdef bytes ipc_bytes = sink.getvalue().to_pybytes()
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string c_ipc = ipc_bytes
        cdef CObjectTransferInfo info = self._store.PutFromIPCAndGetTransferInfo(
            c_key, c_ipc)
        return {
            "flight_uri": info.flight_uri.decode("utf-8"),
            "key": info.key.decode("utf-8"),
            "pid": info.pid,
            "ipc_size": info.ipc_size,
        }

    def get_local(self, str key):
        """Get a locally stored table as IPC bytes, deserialize in Python."""
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.GetLocalAsIPC(c_key)
        if ipc_bytes.empty():
            return None
        reader = pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def fetch(self, str uri, str key):
        """Fetch a table from a remote Flight server, return as PyArrow Table."""
        cdef c_string c_uri = uri.encode("utf-8")
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.FetchAsIPC(c_uri, c_key)
        reader = pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def fetch_via_vm(self, str flight_uri, str key, long long ipc_size):
        """Fetch a table from a same-node producer using process_vm_writev."""
        cdef c_string c_uri = flight_uri.encode("utf-8")
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.FetchViaVMAsIPC(
            c_uri, c_key, ipc_size)
        reader = pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def delete(self, str key):
        cdef c_string c_key = key.encode("utf-8")
        self._store.Delete(c_key)

    def size(self) -> int:
        return self._store.Size()


# ---------------------------------------------------------------------------
# Per-worker singleton
# ---------------------------------------------------------------------------

import os as _os

_global_store = None


def get_flight_store():
    global _global_store
    if _global_store is None:
        _global_store = PyArrowFlightStore()
        _global_store.start_server()
    return _global_store


def is_flight_store_enabled():
    return _os.environ.get("RAY_USE_FLIGHT_STORE", "0") == "1"
