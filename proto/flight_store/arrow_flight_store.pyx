# cython: language_level = 3
# distutils: language = c++

from arrow_flight_store cimport (
    CArrowFlightStore, CTable, CObjectTransferInfo, pid_t,
)
from libcpp.memory cimport shared_ptr, make_shared, unique_ptr
from libcpp.string cimport string as c_string

# Use PyArrow's C++ interop for zero-copy table exchange.
# pyarrow.lib exposes unwrap_table / wrap_table via Cython.
from pyarrow.lib cimport (
    CTable as CPyArrowTable,
    pyarrow_unwrap_table,
    pyarrow_wrap_table,
)

import pyarrow as pa


cdef class PyArrowFlightStore:
    """Python wrapper around the C++ ArrowFlightStore.

    Provides a Flight-RPC-based object store where Arrow tables are stored
    in the C++ heap and served via a C++ Flight server (no GIL overhead).
    """
    cdef CArrowFlightStore *store

    def __cinit__(self):
        self.store = new CArrowFlightStore()

    def __dealloc__(self):
        if self.store != NULL:
            del self.store
            self.store = NULL

    def start_server(self) -> int:
        """Start the Flight server. Returns the port."""
        return self.store.StartServer()

    def stop_server(self):
        """Stop the Flight server."""
        self.store.StopServer()

    def get_uri(self) -> str:
        """Get the Flight URI (grpc://ip:port)."""
        return self.store.GetUri().decode("utf-8")

    def put(self, str key, table):
        """Store a PyArrow Table by key (zero-copy to C++).

        Also pre-serializes to IPC bytes for same-node VM transfers.
        """
        if not isinstance(table, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        cdef shared_ptr[CTable] c_table = pyarrow_unwrap_table(table)
        cdef c_string c_key = key.encode("utf-8")
        self.store.Put(c_key, c_table)

    def put_and_get_transfer_info(self, str key, table):
        """Store a table and return transfer info for same-node VM fetch.

        Returns dict with: flight_uri, key, pid, ipc_address, ipc_size.
        """
        if not isinstance(table, pa.Table):
            raise TypeError(f"Expected pyarrow.Table, got {type(table)}")
        cdef shared_ptr[CTable] c_table = pyarrow_unwrap_table(table)
        cdef c_string c_key = key.encode("utf-8")
        cdef CObjectTransferInfo info = self.store.PutAndGetTransferInfo(
            c_key, c_table)
        return {
            "flight_uri": info.flight_uri.decode("utf-8"),
            "key": info.key.decode("utf-8"),
            "pid": info.pid,
            "ipc_address": info.ipc_address,
            "ipc_size": info.ipc_size,
        }

    def get_local(self, str key):
        """Get a locally stored table. Returns None if not found."""
        cdef c_string c_key = key.encode("utf-8")
        cdef shared_ptr[CTable] c_table = self.store.GetLocal(c_key)
        if c_table.get() == NULL:
            return None
        return pyarrow_wrap_table(c_table)

    def fetch(self, str uri, str key):
        """Fetch a table from a remote Flight server.

        The actual Flight RPC runs in C++ with the GIL released.
        """
        cdef c_string c_uri = uri.encode("utf-8")
        cdef c_string c_key = key.encode("utf-8")
        cdef shared_ptr[CTable] c_table
        with nogil:
            result = self.store.Fetch(c_uri, c_key)
        if not result.ok():
            raise RuntimeError(f"Flight fetch failed for {key} from {uri}")
        c_table = result.ValueUnsafe()
        return pyarrow_wrap_table(c_table)

    def fetch_via_vm(self, int pid, unsigned long ipc_address, long long ipc_size):
        """Fetch a table from a same-node producer using process_vm_readv.

        Single syscall, no gRPC, no GIL during the transfer.
        Requires same-user permissions (or CAP_SYS_PTRACE).
        """
        cdef shared_ptr[CTable] c_table
        with nogil:
            result = CArrowFlightStore.FetchViaVM(
                <pid_t>pid, ipc_address, ipc_size)
        if not result.ok():
            raise RuntimeError(
                f"process_vm_readv fetch failed (pid={pid}, "
                f"addr=0x{ipc_address:x}, size={ipc_size})")
        c_table = result.ValueUnsafe()
        return pyarrow_wrap_table(c_table)

    def delete(self, str key):
        """Delete a stored table."""
        cdef c_string c_key = key.encode("utf-8")
        self.store.Delete(c_key)

    def size(self) -> int:
        """Number of stored tables."""
        return self.store.Size()
