"""Arrow Flight RDT backend.

One-sided transport for pyarrow.Table objects:
  - Source actor runs a Flight server (via the shared FlightCore) and holds
    tables indexed by obj_id.
  - Destination actor allocates a local Arrow buffer and asks the source,
    via a Flight DoAction, to serialize the table's IPC stream directly
    into that buffer using process_vm_writev (Linux only).
  - Cross-node destinations fall back to Flight DoGet (regular RPC).

Registered with RDT as `ARROW_FLIGHT`, `["cpu"]`, data_type=pyarrow.Table.

The Flight server, table storage, and scatter-write/fetch primitives are
shared with the legacy "native" Flight store
(python/ray/_private/flight_object_store.py) via
python/ray/_private/flight_core.py.
"""

import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

import ray
from ray._private.flight_core import get_flight_core
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass
class ArrowFlightCommunicatorMetadata(CommunicatorMetadata):
    """Empty — Arrow Flight is one-sided, no coordination state needed."""


@dataclass
class ArrowFlightTransportMetadata(TensorTransportMetadata):
    flight_uri: Optional[str] = None
    key: Optional[str] = None
    pid: Optional[int] = None
    node_id: Optional[str] = None
    ipc_size: Optional[int] = None


class ArrowFlightTransport(TensorTransportManager):
    def __init__(self):
        self._core = get_flight_core()

    # ------------------------------------------------------------------ RDT API

    def tensor_transport_backend(self) -> str:
        return "ARROW_FLIGHT"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return False

    def actor_has_tensor_transport(self, actor) -> bool:
        return True

    def extract_tensor_transport_metadata(
        self, obj_id: str, rdt_object: List["pa.Table"]
    ) -> ArrowFlightTransportMetadata:
        uri = self._core.ensure_server()

        if not rdt_object:
            return ArrowFlightTransportMetadata(
                tensor_meta=[],
                tensor_device="cpu",
                flight_uri=uri,
                key=obj_id,
                pid=os.getpid(),
                node_id=ray.get_runtime_context().get_node_id(),
                ipc_size=0,
            )

        # We only support a single table per RDT object. Ray's serializer
        # walks the return value and extracts every registered-type instance
        # into the rdt_object list, so >1 means the user returned multiple
        # tables — not supported by this transport.
        if len(rdt_object) != 1:
            raise ValueError(
                "ArrowFlightTransport only supports a single pa.Table per "
                f"RDT object; got {len(rdt_object)}"
            )
        [table] = rdt_object
        size = self._core.put(obj_id, table)

        return ArrowFlightTransportMetadata(
            tensor_meta=[((table.num_rows,), str(table.schema))],
            tensor_device="cpu",
            flight_uri=uri,
            key=obj_id,
            pid=os.getpid(),
            node_id=ray.get_runtime_context().get_node_id(),
            ipc_size=size,
        )

    def get_communicator_metadata(
        self, src_actor, dst_actor, backend: Optional[str] = None
    ) -> ArrowFlightCommunicatorMetadata:
        return ArrowFlightCommunicatorMetadata()

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: ArrowFlightTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List[Any]] = None,
    ) -> List["pa.Table"]:
        meta = tensor_transport_metadata
        if not meta.ipc_size:
            return []

        my_node = ray.get_runtime_context().get_node_id()
        same_node = meta.node_id == my_node and sys.platform == "linux"
        if same_node:
            table = self._core.fetch_via_vm(meta.flight_uri, meta.key, meta.ipc_size)
        else:
            table = self._core.fetch_via_flight(meta.flight_uri, meta.key)
        return [table]

    def send_multiple_tensors(self, tensors, meta, comm):
        raise NotImplementedError(
            "ArrowFlightTransport is one-sided; send_multiple_tensors is unused"
        )

    def garbage_collect(self, obj_id: str, meta, tensors):
        self._core.delete(obj_id)

    def abort_transport(self, obj_id: str, communicator_metadata):
        pass
