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

import fcntl
import logging
import math
import os
import random
import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

import ray
from ray.experimental.rdt.tensor_transport_manager import (
    CommunicatorMetadata,
    FetchRequest,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch

logger = logging.getLogger(__name__)

# Name of the singleton Ray actor that hosts WeightSynchronizerManager when
# no external RAIDEN_MANAGER_ADDRESS is configured.
_WSM_COORDINATOR_ACTOR_NAME = "_ray_rdt_tpu_sync_wsm_coordinator"
_WSM_COORDINATOR_NAMESPACE = "ray_internal_rdt"


def _get_ip_for_interface(ifname: str) -> Optional[str]:
    """Queries the IPv4 address bound to a specific network interface (e.g. eth1, eth2)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        return socket.inet_ntoa(
            fcntl.ioctl(
                s.fileno(),
                0x8915,  # SIOCGIFADDR
                struct.pack("256s", ifname[:15].encode("utf-8")),
            )[20:24]
        )
    except Exception:
        return None
    finally:
        s.close()


def _get_pod_control_ip() -> str:
    """Returns the primary pod/host IP address used for control-plane RPCs."""
    for ifname in ("eth0", "enp0s1", "ens4"):
        ip = _get_ip_for_interface(ifname)
        if ip and not ip.startswith("127."):
            return ip
    try:
        ip = socket.gethostbyname(socket.gethostname())
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return "127.0.0.1"


def _get_data_plane_ips() -> List[str]:
    """Discovers high-speed data-plane IPs (preferring GKE DraNet eth1/eth2 if present)."""
    ips = []
    for ifname in ("eth1", "eth2"):
        ip = _get_ip_for_interface(ifname)
        if ip and not ip.startswith("127.") and ip not in ips:
            ips.append(ip)
    if not ips:
        ips.append(_get_pod_control_ip())
    return ips


class _WsmCoordinatorActor:
    """Cluster-wide singleton Ray actor hosting TPU Sync's WeightSynchronizerManager."""

    def __init__(self):
        from tpu_sync.api.weight_synchronizer_manager import WeightSynchronizerManager
        from tpu_sync.rpc.raiden_controller import WeightSyncWorkerRpcClient

        self._rpc_client = WeightSyncWorkerRpcClient(name_resolver=None)
        self._wsm = WeightSynchronizerManager(
            port=0,
            worker_rpc_client=self._rpc_client,
            enable_plan_cache=True,
            auto_start_server=True,
        )
        self._ip = _get_pod_control_ip()
        self._address = f"{self._ip}:{self._wsm.port}"
        logger.info(
            f"[_WsmCoordinatorActor] Started WeightSynchronizerManager server at {self._address}"
        )

    def get_address(self) -> str:
        return self._address


def _get_or_create_cluster_wsm_address() -> str:
    """Returns the address of the cluster's WeightSynchronizerManager server.

    Checks RAIDEN_MANAGER_ADDRESS first; otherwise lazily creates or looks up
    the singleton _WsmCoordinatorActor in the Ray cluster.
    """
    env_addr = os.environ.get("RAIDEN_MANAGER_ADDRESS")
    if env_addr:
        return env_addr

    try:
        actor = ray.get_actor(
            _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
        )
    except ValueError:
        try:
            remote_cls = ray.remote(num_cpus=0)(_WsmCoordinatorActor)
            actor = remote_cls.options(
                name=_WSM_COORDINATOR_ACTOR_NAME,
                namespace=_WSM_COORDINATOR_NAMESPACE,
                lifetime="detached",
                get_if_exists=True,
            ).remote()
        except Exception:
            actor = ray.get_actor(
                _WSM_COORDINATOR_ACTOR_NAME, namespace=_WSM_COORDINATOR_NAMESPACE
            )
    return ray.get(actor.get_address.remote())


def _build_variable_protos_from_tensor_meta(
    tensor_meta: List[Tuple[Any, Any]],
) -> List[Any]:
    """Builds VariableMetadataProto descriptors for a list of (shape, dtype) pairs."""
    from tpu_sync.rpc import raiden_service_pb2

    protos = []
    for idx, (shape, dtype) in enumerate(tensor_meta):
        shape_list = [int(d) for d in shape]
        if not shape_list:
            shape_list = [1]
        ndim = len(shape_list)
        layout = list(range(ndim - 1, -1, -1))
        m_shape = [1] * ndim
        spec_axes = [""] * ndim
        item_size = getattr(dtype, "itemsize", 4)

        protos.append(
            raiden_service_pb2.VariableMetadataProto(
                name=f"tensor_{idx}",
                shape=shape_list,
                mesh_shape=m_shape,
                layout=layout,
                item_size=item_size,
                layer_idx=idx,
                sharding_spec=spec_axes,
            )
        )
    return protos


@dataclass
class TpuSyncCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the TPU Sync communicator (one-sided)."""

    manager_address: Optional[str] = None
    parallelism: int = 4


@dataclass
class TpuSyncTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors registered with TPU Sync and WeightSynchronizerManager.

    Args:
        src_job_name: Source job name in RaidenId.
        src_replica_id: Source replica identifier in RaidenId.
        src_data_name: Source data name (obj_id) in RaidenId.
        manager_address: Address ('ip:port') of the WeightSynchronizerManager server.
        data_endpoints: Physical TCP data endpoints exposed by the sender.
        control_endpoint: Control-plane RPC endpoint of the sender's C++ listener.
        total_bytes: Total physical payload size across all tensors in bytes.
        mesh_shape: Logical mesh shape of the source work unit.
    """

    src_job_name: str = "rdt_src"
    src_replica_id: str = "0"
    src_data_name: str = ""
    manager_address: Optional[str] = None
    data_endpoints: List[str] = field(default_factory=list)
    control_endpoint: Optional[str] = None
    total_bytes: int = 0
    mesh_shape: List[int] = field(default_factory=lambda: [1])


@dataclass
class TpuSyncFetchRequest(FetchRequest):
    """TPU Sync FetchRequest carrying the transfer state for wait_fetch_complete."""

    transfer_uuid: int = 0
    receiver_ws: Any = None
    manager_client: Any = None
    dst_device: Optional[str] = None
    staging_buffers: Optional[List["torch.Tensor"]] = None


class TpuSyncTensorTransport(TensorTransportManager):
    """One-sided RDT TensorTransportManager backed by TPU Sync's WeightSynchronizerManager."""

    def __init__(
        self,
        manager_address: Optional[str] = None,
        default_parallelism: int = 4,
    ):
        self._manager_address = manager_address or os.environ.get(
            "RAIDEN_MANAGER_ADDRESS"
        )
        self._parallelism = default_parallelism
        self._lock = threading.RLock()
        # Active sender WeightSynchronizer instances keyed by obj_id
        self._active_senders: Dict[str, Any] = {}
        # Active receiver WeightSynchronizer instances keyed by obj_id
        self._active_receivers: Dict[str, Any] = {}
        # Cached RaidenControllerClientFacade instances keyed by manager_address
        self._wsm_clients: Dict[str, Any] = {}
        self._aborted_obj_ids: set = set()

    def tensor_transport_backend(self) -> str:
        return "TPU_SYNC"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        def __ray_actor_has_tpu_sync__(self_actor: "ray.actor.ActorHandle") -> bool:
            try:
                import tpu_sync  # noqa: F401

                return True
            except Exception:
                return False

        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tpu_sync__
            )
        )

    def _get_manager_address(self) -> str:
        with self._lock:
            if not self._manager_address:
                self._manager_address = _get_or_create_cluster_wsm_address()
            return self._manager_address

    def _get_wsm_client(self, manager_address: Optional[str] = None):
        from tpu_sync.rpc.raiden_controller import RaidenControllerClientFacade

        addr = manager_address or self._get_manager_address()
        with self._lock:
            client = self._wsm_clients.get(addr)
            if client is None:
                client = RaidenControllerClientFacade(addr)
                self._wsm_clients[addr] = client
            return client

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        rdt_object: List["torch.Tensor"],
    ) -> TpuSyncTransportMetadata:
        """Called on the source actor/driver upon tensor creation or ray.put.

        1. Starts a WeightSynchronizer with both data-plane (local_port=0) and
           control-plane (listener_port=0) servers.
        2. Initiates asynchronous D2H copy to host staging buffers.
        3. Registers the source work unit and variable descriptors with
           WeightSynchronizerManager.
        4. Returns TpuSyncTransportMetadata so any receiver can pull one-sidedly.
        """
        import torch
        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer
        from tpu_sync.rpc.raiden_controller import RaidenId

        tensor_meta: List[Tuple[Any, Any]] = []
        device_type: Optional[str] = None
        total_bytes = 0

        if not rdt_object:
            return TpuSyncTransportMetadata(
                tensor_meta=[],
                tensor_device=None,
                src_data_name=obj_id,
            )

        for t in rdt_object:
            if not t.is_contiguous():
                raise ValueError("All tensors in an RDT object must be contiguous.")
            tensor_meta.append((t.shape, t.dtype))
            total_bytes += t.nelement() * t.element_size()
            if device_type is None:
                device_type = t.device.type
            elif device_type != t.device.type:
                raise ValueError(
                    "All tensors in an RDT object must have the same device type."
                )

        # Synchronize TPU operations before D2H
        if device_type in ("tpu", "xla"):
            try:
                from torch_tpu._internal import sync

                sync.synchronize(rdt_object, wait=True)
            except Exception:
                try:
                    torch.tpu.synchronize()
                except Exception:
                    pass

        grouped_tensors = [[t] for t in rdt_object]

        os.environ.setdefault("ENABLE_MULTI_NUMA", "1")
        ws = WeightSynchronizer(
            device_tensors=grouped_tensors,
            local_port=0,
            listener_port=0,
            parallelism=self._parallelism,
            bind_ip=None,
            auto_h2d=False,
        )

        # Trigger D2H copy immediately so weights are staged in host memory
        ws.d2h()

        control_ip = _get_pod_control_ip()
        control_endpoint = f"{control_ip}:{ws.listener_port}"

        eps = ws.get_local_endpoints()
        data_endpoints = []
        for ep in eps:
            if ep.get("endpoint"):
                data_endpoints.append(ep["endpoint"])
        if not data_endpoints:
            data_ips = _get_data_plane_ips()
            data_endpoints = [f"{data_ips[0]}:{ws.local_port}"]

        # Use the first data endpoint for the single shard per layer
        shard_endpoints = [data_endpoints[0]]

        manager_addr = self._get_manager_address()
        wsm_client = self._get_wsm_client(manager_addr)

        src_job = f"rdt_src_{obj_id[:12]}"
        src_replica = "0"
        src_unit = RaidenId(
            job_name=src_job,
            job_replica_id=src_replica,
            data_name=obj_id,
        )

        var_protos = _build_variable_protos_from_tensor_meta(tensor_meta)

        wsm_client.register_work_unit(
            unit=src_unit,
            shards=shard_endpoints,
            control_plane_rpc_address=control_endpoint,
            mesh_shape=[1],
            mesh_axes=["fsdp"],
            variables=var_protos,
            host_subgrid=[1],
        )

        with self._lock:
            self._active_senders[obj_id] = ws

        logger.info(
            f"[TpuSyncTensorTransport] Registered one-sided source unit {src_unit} "
            f"with WSM ({manager_addr}), control={control_endpoint}, "
            f"shards={shard_endpoints}, bytes={total_bytes}"
        )

        return TpuSyncTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device_type or "tpu",
            src_job_name=src_job,
            src_replica_id=src_replica,
            src_data_name=obj_id,
            manager_address=manager_addr,
            data_endpoints=shard_endpoints,
            control_endpoint=control_endpoint,
            total_bytes=total_bytes,
            mesh_shape=[1],
        )

    def get_communicator_metadata(
        self,
        src_actor: Optional["ray.actor.ActorHandle"],
        dst_actor: Optional["ray.actor.ActorHandle"],
        backend: Optional[str] = None,
    ) -> TpuSyncCommunicatorMetadata:
        return TpuSyncCommunicatorMetadata(
            manager_address=self._manager_address,
            parallelism=self._parallelism,
        )

    def fetch_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> TpuSyncFetchRequest:
        """Initiates a one-sided transfer orchestrated by WeightSynchronizerManager.

        1. Allocates target TPU (or CPU) buffers if not provided.
        2. Starts receiver WeightSynchronizer with auto_h2d=True.
        3. Registers destination work unit with WeightSynchronizerManager.
        4. Calls WSM coordinate_transfer(src_units, dst_units, use_block_chunks=True).
        5. Returns TpuSyncFetchRequest.
        """
        import torch
        from tpu_sync.api.torch.weight_synchronizer import WeightSynchronizer
        from tpu_sync.rpc.raiden_controller import RaidenId, RaidenMemoryType

        assert isinstance(tensor_transport_metadata, TpuSyncTransportMetadata)
        assert isinstance(communicator_metadata, TpuSyncCommunicatorMetadata)

        with self._lock:
            if obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.discard(obj_id)
                raise RuntimeError(f"TPU_SYNC transfer aborted for obj_id={obj_id}")

        if not tensor_transport_metadata.tensor_meta:
            return TpuSyncFetchRequest(obj_id=obj_id, tensors=[])

        dev_str = tensor_transport_metadata.tensor_device or "tpu"
        if target_buffers is not None:
            buffers = target_buffers
            actual_dev_type = buffers[0].device.type
        else:
            try:
                dev = torch.device(dev_str)
                buffers = [
                    torch.zeros(shape, dtype=dtype, device=dev)
                    for shape, dtype in tensor_transport_metadata.tensor_meta
                ]
                actual_dev_type = dev.type
            except Exception:
                # Fallback to CPU if TPU device is unavailable on this process (e.g. CPU head node calling ray.get)
                dev = torch.device("cpu")
                buffers = [
                    torch.zeros(shape, dtype=dtype, device=dev)
                    for shape, dtype in tensor_transport_metadata.tensor_meta
                ]
                actual_dev_type = "cpu"

        # If buffers are on CPU (e.g. driver ray.get on CPU node), check if TPU is available
        # for WeightSynchronizer binding; if TPU is available, stage via TPU or bind directly.
        tpu_buffers = buffers
        staging_tpu_buffers = None
        if actual_dev_type == "cpu":
            try:
                tpu_dev = torch.device("tpu:0")
                staging_tpu_buffers = [
                    torch.zeros(shape, dtype=dtype, device=tpu_dev)
                    for shape, dtype in tensor_transport_metadata.tensor_meta
                ]
                tpu_buffers = staging_tpu_buffers
            except Exception:
                pass

        if tpu_buffers[0].device.type in ("tpu", "xla"):
            try:
                from torch_tpu._internal import sync

                sync.synchronize(tpu_buffers, wait=True)
            except Exception:
                try:
                    torch.tpu.synchronize()
                except Exception:
                    pass

        grouped_buffers = [[t] for t in tpu_buffers]
        os.environ.setdefault("ENABLE_MULTI_NUMA", "1")
        dst_ws = WeightSynchronizer(
            device_tensors=grouped_buffers,
            local_port=0,
            listener_port=0,
            parallelism=self._parallelism,
            bind_ip=None,
            auto_h2d=True,
        )

        control_ip = _get_pod_control_ip()
        dst_control_endpoint = f"{control_ip}:{dst_ws.listener_port}"

        eps = dst_ws.get_local_endpoints()
        dst_data_endpoints = []
        for ep in eps:
            if ep.get("endpoint"):
                dst_data_endpoints.append(ep["endpoint"])
        if not dst_data_endpoints:
            data_ips = _get_data_plane_ips()
            dst_data_endpoints = [f"{data_ips[0]}:{dst_ws.local_port}"]

        dst_shard_endpoints = [dst_data_endpoints[0]]

        manager_addr = (
            tensor_transport_metadata.manager_address
            or communicator_metadata.manager_address
            or self._get_manager_address()
        )
        wsm_client = self._get_wsm_client(manager_addr)

        transfer_uuid = random.randint(100000, 999999999)
        dst_job = f"rdt_dst_{obj_id[:12]}_{transfer_uuid}"
        dst_unit = RaidenId(
            job_name=dst_job,
            job_replica_id="0",
            data_name=obj_id,
        )
        src_unit = RaidenId(
            job_name=tensor_transport_metadata.src_job_name,
            job_replica_id=tensor_transport_metadata.src_replica_id,
            data_name=tensor_transport_metadata.src_data_name,
        )

        var_protos = _build_variable_protos_from_tensor_meta(
            tensor_transport_metadata.tensor_meta
        )

        wsm_client.register_work_unit(
            unit=dst_unit,
            shards=dst_shard_endpoints,
            control_plane_rpc_address=dst_control_endpoint,
            mesh_shape=[1],
            mesh_axes=["fsdp"],
            variables=var_protos,
            host_subgrid=[1],
        )

        with self._lock:
            self._active_receivers[obj_id] = dst_ws

        logger.info(
            f"[TpuSyncTensorTransport] Triggering WSM coordinate_transfer: "
            f"{src_unit} -> {dst_unit} (uuid={transfer_uuid})"
        )

        # Coordinate transfer through WSM: WSM computes the reshard plan, arms the
        # receiver's C++ listener with expected chunk counts, and commands the
        # sender's C++ listener to stream tensor chunks to the receiver.
        wsm_client.coordinate_transfer(
            src_units=[src_unit],
            dst_units=[dst_unit],
            use_block_chunks=True,
            is_sender=True,
            uuid=transfer_uuid,
            dst_mem_type=RaidenMemoryType.DRAM,
        )

        return TpuSyncFetchRequest(
            obj_id=obj_id,
            tensors=buffers,
            transfer_uuid=transfer_uuid,
            receiver_ws=dst_ws,
            manager_client=wsm_client,
            dst_device=actual_dev_type,
            staging_buffers=staging_tpu_buffers,
        )

    def wait_fetch_complete(
        self,
        fetch_request: FetchRequest,
        timeout: float = -1,
    ) -> List["torch.Tensor"]:
        """Waits for the C++ WeightSynchronizer to complete data ingestion and H2D DMA."""
        import torch

        assert isinstance(fetch_request, TpuSyncFetchRequest)
        if not fetch_request.tensors:
            return []

        obj_id = fetch_request.obj_id
        with self._lock:
            if obj_id in self._aborted_obj_ids:
                self._aborted_obj_ids.discard(obj_id)
                raise RuntimeError(f"TPU_SYNC transfer aborted for obj_id={obj_id}")

        dst_ws = fetch_request.receiver_ws
        if dst_ws is not None:
            # Wait for all chunks to arrive and auto_h2d to complete
            dst_ws.wait_for_transfer_completion(fetch_request.transfer_uuid)
            try:
                from torch_tpu._internal import sync

                target_tpu_list = (
                    fetch_request.staging_buffers
                    if fetch_request.staging_buffers is not None
                    else fetch_request.tensors
                )
                if target_tpu_list[0].device.type in ("tpu", "xla"):
                    sync.synchronize(target_tpu_list, wait=True)
            except Exception:
                try:
                    torch.tpu.synchronize()
                except Exception:
                    pass

            # If caller requested CPU tensors, copy from host buffer or staging TPU buffers
            if fetch_request.dst_device == "cpu":
                for idx, cpu_tensor in enumerate(fetch_request.tensors):
                    try:
                        host_view = dst_ws.get_host_buffer(layer_idx=idx, shard_idx=0)
                        cpu_tensor.copy_(host_view.view(cpu_tensor.shape))
                    except Exception:
                        if fetch_request.staging_buffers is not None:
                            cpu_tensor.copy_(fetch_request.staging_buffers[idx].cpu())

        with self._lock:
            self._active_receivers.pop(obj_id, None)
        fetch_request.receiver_ws = None

        return fetch_request.tensors

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
        target_buffers: Optional[List["torch.Tensor"]] = None,
    ) -> List["torch.Tensor"]:
        """Receives multiple tensors synchronously via fetch + wait."""
        fetch_request = self.fetch_multiple_tensors(
            obj_id,
            tensor_transport_metadata,
            communicator_metadata,
            target_buffers,
        )
        return self.wait_fetch_complete(fetch_request)

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "TPU_SYNC transport is one-sided and does not support send_multiple_tensors."
        )

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        tensors: List[Any],
    ):
        """Releases active sender/receiver WeightSynchronizer sessions upon ObjectRef GC."""
        with self._lock:
            self._active_senders.pop(obj_id, None)
            self._active_receivers.pop(obj_id, None)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        """Aborts in-progress transfers and cleans up state."""
        with self._lock:
            self._aborted_obj_ids.add(obj_id)
            self._active_senders.pop(obj_id, None)
            self._active_receivers.pop(obj_id, None)
