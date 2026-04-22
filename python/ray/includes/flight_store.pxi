# Arrow Flight store — included in _raylet.pyx.
# Provides PyArrowFlightStore class and get_flight_store() singleton.

import pyarrow as _flight_pa

cdef class PyArrowFlightStore:
    """C++ Arrow Flight-based object store, accessible from Python."""
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
        """Store a PyArrow Table by key (via IPC serialization)."""
        if not isinstance(table, _flight_pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        sink = _flight_pa.BufferOutputStream()
        writer = _flight_pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        cdef bytes ipc_bytes = sink.getvalue().to_pybytes()
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string c_ipc = ipc_bytes
        self._store.PutFromIPC(c_key, c_ipc)

    def put_and_get_transfer_info(self, str key, table):
        """Store a table and return transfer info for VM/Flight fetch."""
        if not isinstance(table, _flight_pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        sink = _flight_pa.BufferOutputStream()
        writer = _flight_pa.ipc.new_stream(sink, table.schema)
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
        """Get a locally stored table."""
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.GetLocalAsIPC(c_key)
        if ipc_bytes.empty():
            return None
        reader = _flight_pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def fetch(self, str uri, str key):
        """Fetch a table from a remote Flight server."""
        cdef c_string c_uri = uri.encode("utf-8")
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.FetchAsIPC(c_uri, c_key)
        reader = _flight_pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def fetch_via_vm(self, str flight_uri, str key, long long ipc_size):
        """Fetch a table from a same-node producer using process_vm_writev."""
        cdef c_string c_uri = flight_uri.encode("utf-8")
        cdef c_string c_key = key.encode("utf-8")
        cdef c_string ipc_bytes = self._store.FetchViaVMAsIPC(
            c_uri, c_key, ipc_size)
        reader = _flight_pa.ipc.open_stream(ipc_bytes)
        return reader.read_all()

    def delete(self, str key):
        cdef c_string c_key = key.encode("utf-8")
        self._store.Delete(c_key)

    def size(self) -> int:
        return self._store.Size()


_flight_store_singleton = None


def get_flight_store():
    """Get or create the per-worker Flight store singleton."""
    global _flight_store_singleton
    if _flight_store_singleton is None:
        _flight_store_singleton = PyArrowFlightStore()
        _flight_store_singleton.start_server()
    return _flight_store_singleton


def is_flight_store_enabled():
    """Check if the Flight store is enabled via RAY_USE_FLIGHT_STORE=1."""
    return os.environ.get("RAY_USE_FLIGHT_STORE", "0") == "1"
