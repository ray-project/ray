# Copyright 2026 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import socket
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)


def _get_pod_ip() -> str:
    """Returns the primary IP of the current pod / host."""
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "127.0.0.1"


@dataclass
class TpuSyncCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for TPU Sync communicator containing peer connection coordinates.

    Args:
        dst_ip: IP address of the destination actor.
        dst_port: Ephemeral TCP listening port of destination WeightSynchronizer.
        parallelism: Number of parallel TCP streams for transfer.
    """

    dst_ip: Optional[str] = None
    dst_port: Optional[int] = None
    parallelism: int = 4
    obj_id: Optional[str] = None


@dataclass
class TpuSyncTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors transported via TPU Sync.

    Args:
        tensor_meta: List of (shape, dtype) tuples for each tensor in the object.
        tensor_device: Device type string (e.g., "tpu", "xla").
        total_bytes: Total physical payload size across all tensors in bytes.
    """

    total_bytes: int = 0


class TpuSyncTensorTransport(TensorTransportManager):
    """RDT TensorTransportManager implementation using Google Cloud TPU Sync."""

    def __init__(self, default_parallelism: int = 4):
        self._parallelism = default_parallelism
        self._lock = threading.Lock()
        # Active WeightSynchronizer sessions on the sender side, keyed by obj_id
        self._active_senders: Dict[str, Any] = {}
        # Active WeightSynchronizer sessions on the receiver side, keyed by obj_id
        self._active_receivers: Dict[str, Any] = {}
        # Ephemeral listening port for this worker
        self._cached_listener_port: Optional[int] = None
        # Persistent receiver WeightSynchronizer with active listening server
        self._active_receiver_ws: Optional[Any] = None

    def tensor_transport_backend(self) -> str:
        return "TPU_SYNC"

    @staticmethod
    def is_one_sided() -> bool:
        # Two-sided active push: Ray orchestrates __ray_send__ on src and __ray_recv__ on dst
        return False

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        return True

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> TpuSyncTransportMetadata:
        """Called on source actor immediately after tensor generation.
        Initializes WeightSynchronizer and initiates asynchronous D2H DMA copy.
        """
        import torch
        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer

        tensor_meta = []
        device_type = None
        total_bytes = 0

        for t in rdt_object:
            tensor_meta.append((t.shape, t.dtype))
            total_bytes += t.nelement() * t.element_size()
            if device_type is None:
                device_type = t.device.type
            elif device_type != t.device.type:
                raise ValueError("All tensors in an RDT object must have the same device type.")

        # PyTorch WeightSynchronizer expects a 2D list: [num_layers][num_shards]
        # For a collection of tensors in RDT, each tensor is a distinct layer/chunk.
        grouped_tensors = [[t] for t in rdt_object]

        logger.info(
            f"[TpuSyncTensorTransport] Creating sender WeightSynchronizer for obj_id={obj_id} "
            f"({len(rdt_object)} tensors, {total_bytes / 1e6:.2f} MB)"
        )

        ws = WeightSynchronizer(
            device_tensors=grouped_tensors,
            local_port=0,
            parallelism=self._parallelism,
            bind_ip="0.0.0.0",
        )

        # Trigger asynchronous D2H copy immediately to overlap with Ray control-plane RPCs
        ws.d2h()

        with self._lock:
            self._active_senders[obj_id] = ws

        return TpuSyncTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device_type or "tpu",
            total_bytes=total_bytes,
        )

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
        tensor_transport_meta: Optional[TensorTransportMetadata] = None,
        obj_id: Optional[str] = None,
    ) -> TpuSyncCommunicatorMetadata:
        """Called on Ray driver/owner process before orchestrating the transfer.
        Queries the destination actor's IP and ephemeral port after starting
        a WeightSynchronizer listener configured for the exact incoming tensor layout.
        """
        def get_listener_coords(self_actor, obj_id, transport_meta):
            from ray.experimental.rdt.util import get_tensor_transport_manager
            transport = get_tensor_transport_manager("TPU_SYNC")
            return transport._prepare_receiver(obj_id, transport_meta)

        dst_ip, dst_port = ray.get(
            dst_actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                get_listener_coords,
                obj_id,
                tensor_transport_meta,
            )
        )

        return TpuSyncCommunicatorMetadata(
            dst_ip=dst_ip,
            dst_port=dst_port,
            parallelism=self._parallelism,
            obj_id=obj_id,
        )

    def _prepare_receiver(
        self,
        obj_id: Optional[str],
        tensor_transport_meta: Optional[TensorTransportMetadata],
    ) -> Tuple[str, int]:
        """Runs on the destination actor to prepare receiving buffers and start
        a listening WeightSynchronizer configured for the incoming tensor dimensions.
        """
        import torch
        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer

        if tensor_transport_meta is None:
            # Fallback for generic metadata query if no tensor metadata is supplied
            dev = torch.device("tpu")
            target_buffers = [torch.zeros((1,), dtype=torch.float32, device=dev)]
        else:
            dev_str = tensor_transport_meta.tensor_device or "tpu"
            dev = torch.device(dev_str)
            target_buffers = [
                torch.zeros(shape, dtype=dtype, device=dev)
                for shape, dtype in tensor_transport_meta.tensor_meta
            ]

        grouped_buffers = [[t] for t in target_buffers]
        ws = WeightSynchronizer(
            device_tensors=grouped_buffers,
            local_port=0,
            parallelism=self._parallelism,
            bind_ip="0.0.0.0",
        )
        listener_port = ws.local_port
        logger.info(
            f"[TpuSyncTensorTransport] Prepared receiver for obj_id={obj_id} on port {listener_port} "
            f"with {len(target_buffers)} tensors"
        )

        with self._lock:
            key = obj_id if obj_id is not None else "_default"
            self._active_receivers[key] = (ws, target_buffers)

        return _get_pod_ip(), listener_port

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        """Runs on destination actor via __ray_recv__.
        Retrieves the prepared WeightSynchronizer session, awaits data push, and runs H2D.
        """
        assert isinstance(tensor_transport_metadata, TpuSyncTransportMetadata)
        assert isinstance(communicator_metadata, TpuSyncCommunicatorMetadata)

        with self._lock:
            receiver_entry = self._active_receivers.get(obj_id)

        if receiver_entry is not None:
            ws, buffers = receiver_entry
        else:
            # Fallback if not pre-allocated in _prepare_receiver
            import torch
            from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer

            dev_str = tensor_transport_metadata.tensor_device or "tpu"
            dev = torch.device(dev_str)
            if target_buffers is None:
                buffers = [
                    torch.zeros(shape, dtype=dtype, device=dev)
                    for shape, dtype in tensor_transport_metadata.tensor_meta
                ]
            else:
                buffers = target_buffers
            ws = WeightSynchronizer(
                device_tensors=[[t] for t in buffers],
                local_port=communicator_metadata.dst_port,
                parallelism=communicator_metadata.parallelism,
                bind_ip="0.0.0.0",
            )
            with self._lock:
                self._active_receivers[obj_id] = (ws, buffers)

        logger.info(f"[TpuSyncTensorTransport] Performing H2D for obj_id={obj_id}")
        ws.h2d()
        logger.info(f"[TpuSyncTensorTransport] H2D completed for obj_id={obj_id}")

        return buffers

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        """Runs on source actor via __ray_send__.
        Pushes staged host weights to the destination actor's listening port.
        """
        assert isinstance(tensor_transport_metadata, TpuSyncTransportMetadata)
        assert isinstance(communicator_metadata, TpuSyncCommunicatorMetadata)

        with self._lock:
            ws = self._active_senders.get(communicator_metadata.obj_id)
            if ws is None:
                for key, val in self._active_senders.items():
                    if val is not None:
                        ws = val
                        break

        if ws is None:
            from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer
            grouped_tensors = [[t] for t in tensors]
            ws = WeightSynchronizer(
                device_tensors=grouped_tensors,
                local_port=0,
                parallelism=communicator_metadata.parallelism,
                bind_ip="0.0.0.0",
            )
            ws.d2h()

        peer_endpoint = f"{communicator_metadata.dst_ip}:{communicator_metadata.dst_port}"
        logger.info(f"[TpuSyncTensorTransport] Pushing weights to peer: {peer_endpoint}")
        max_retries = 10
        for attempt in range(max_retries):
            try:
                ws.push_weights([peer_endpoint])
                logger.info(f"[TpuSyncTensorTransport] Successfully pushed weights to {peer_endpoint} on attempt {attempt+1}")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"[TpuSyncTensorTransport] Failed to push weights to {peer_endpoint} after {max_retries} attempts: {e}")
                    raise
                import time
                time.sleep(0.5)

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ):
        """Releases active synchronizer state upon Ray ObjectRef GC."""
        with self._lock:
            if obj_id in self._active_senders:
                del self._active_senders[obj_id]
            if obj_id in self._active_receivers:
                del self._active_receivers[obj_id]

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """Aborts in-flight transfers and frees pending state."""
        with self._lock:
            self._active_senders.pop(obj_id, None)
            self._active_receivers.pop(obj_id, None)
