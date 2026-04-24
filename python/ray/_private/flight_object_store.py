"""Legacy "native" Flight object store.

When RAY_USE_FLIGHT_NATIVE=1 is set, `_raylet.store_task_outputs` intercepts
pa.Table returns and routes them through this store instead of plasma. The
consumer side deserializes via `serialization._fetch_flight_table`.

The Flight server / storage / scatter-write primitives live in
python/ray/_private/flight_core.py and are shared with the RDT backend.
This module wraps them with the put/fetch API the raylet intercept and
deserializer expect.
"""

import os
import sys
import threading

from ray._private.flight_core import FlightCore, get_flight_core


def is_flight_native_enabled() -> bool:
    return os.environ.get("RAY_USE_FLIGHT_NATIVE", "0") == "1"


class FlightObjectStore:
    """Thin adapter exposing put/fetch semantics over FlightCore."""

    def __init__(self, core: FlightCore):
        self._core = core

    def ensure_server(self) -> str:
        return self._core.ensure_server()

    def get_uri(self):
        return self._core.uri

    def put_and_get_transfer_info(self, key: str, table) -> dict:
        """Store a table and return the transfer info dict embedded into the
        Ray object store as the return value."""
        import ray

        self._core.ensure_server()
        size = self._core.put(key, table)
        return {
            "flight_uri": self._core.uri,
            "key": key,
            "pid": os.getpid(),
            "ipc_size": size,
            "node_id": ray.get_runtime_context().get_node_id(),
        }

    def fetch(self, info: dict):
        """Consumer path: dispatch same-node vs cross-node based on the
        transfer info dict written by put_and_get_transfer_info."""
        import ray

        flight_uri = info["flight_uri"]
        key = info["key"]
        ipc_size = info.get("ipc_size", 0) or 0

        if ipc_size <= 0:
            return self._core.fetch_via_flight(flight_uri, key)

        producer_node = info.get("node_id", "")
        if isinstance(producer_node, bytes):
            producer_node = producer_node.decode("utf-8", errors="replace")
        my_node = ray.get_runtime_context().get_node_id()
        same_node = producer_node == my_node and sys.platform == "linux"

        if same_node:
            table = self._core.fetch_via_vm(flight_uri, key, ipc_size)
        else:
            table = self._core.fetch_via_flight(flight_uri, key)

        # Native path owns its own eviction; tell the producer to drop
        # the entry now that we've pulled it.
        self._core.send_delete_rpc(flight_uri, key)
        return table

    def delete(self, key: str) -> None:
        self._core.delete(key)


_store = None
_store_lock = threading.Lock()


def get_flight_store() -> FlightObjectStore:
    """Per-process singleton. Call only when `is_flight_native_enabled()`."""
    global _store
    if _store is not None:
        return _store
    with _store_lock:
        if _store is None:
            _store = FlightObjectStore(get_flight_core())
    return _store
