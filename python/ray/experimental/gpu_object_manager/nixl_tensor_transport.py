import threading
import time
import traceback
from collections import OrderedDict
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

import ray
from ray._private.ray_constants import NIXL_REMOTE_AGENT_CACHE_MAXSIZE
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    CommunicatorMetadata,
    TensorTransportManager,
    TensorTransportMetadata,
)

if TYPE_CHECKING:
    import torch


@dataclass
class NixlCommunicatorMetadata(CommunicatorMetadata):
    """Metadata for the NIXL communicator."""


@dataclass
class NixlTransportMetadata(TensorTransportMetadata):
    """Metadata for tensors stored in the GPU object store for NIXL transport.

    Args:
        nixl_serialized_descs: Serialized tensor descriptors for NIXL transport.
        nixl_agent_meta: The additional metadata of the remote NIXL agent.
        nixl_agent_name: The name of the NIXL agent.
        reset_remote_agent: Whether to reset the remote NIXL agent (when receving the metadata).
    """

    nixl_reg_descs: Optional[Any] = None
    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None
    nixl_agent_name: Optional[str] = None
    reset_remote_agent: Optional[bool] = None

    __eq__ = object.__eq__
    __hash__ = object.__hash__


class NixlTensorTransport(TensorTransportManager):
    def __init__(self):
        # This is lazily initialized because it requires NIXL to actually be installed and we want to allow an owner that is just coordinating to not need to have NIXL installed.
        self._nixl_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()

        # LRU cache of remote agent names. When full, the least
        # recently used remote agent is evicted and remove_remote_agent is called.
        self._remote_agents: OrderedDict = OrderedDict()
        self._nixl_memory_lock = threading.Lock()
        # Label showing whether there has been any nixl memory deregistered.
        self._memory_deregistered = False

    def tensor_transport_backend(self) -> str:
        return "NIXL"

    @staticmethod
    def is_one_sided() -> bool:
        return True

    @staticmethod
    def can_abort_transport() -> bool:
        return True

    def get_nixl_agent(self):
        """
        Creates a NIXL agent with UCX backend if not already created.
        """
        if self._nixl_agent is not None:
            return self._nixl_agent

        from nixl._api import nixl_agent, nixl_agent_config

        agent_config = nixl_agent_config(backends=["UCX"])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            # If the actor id is None, it means the current process is a driver.
            import uuid

            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        self._nixl_agent = nixl_agent(actor_id, agent_config)
        return self._nixl_agent

    def actor_has_tensor_transport(self, actor: "ray.actor.ActorHandle") -> bool:
        # TODO(dayshah): This is called on a .remote RDT call, so it's quite expensive.
        def __ray_actor_has_tensor_transport__(
            self: "ray.actor.ActorHandle",
        ) -> bool:
            # Check if nixl is installed
            try:
                from ray.experimental.gpu_object_manager.util import (
                    get_tensor_transport_manager,
                )

                get_tensor_transport_manager("NIXL").get_nixl_agent()
                return True
            except Exception:
                return False

        return ray.get(
            actor.__ray_call__.options(concurrency_group="_ray_system").remote(
                __ray_actor_has_tensor_transport__
            )
        )

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        gpu_object: List["torch.Tensor"],
    ) -> NixlTransportMetadata:
        import torch

        from ray._private.worker import global_worker

        gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
        device = None
        tensor_meta = []
        duplicate_meta = gpu_object_store.record_and_get_meta_if_duplicate(
            obj_id, gpu_object
        )
        if duplicate_meta is not None:
            return duplicate_meta

        if gpu_object:
            # We assume all tensors in one GPU object have the same device type, but we
            # don't assume they're all on the same device.
            devices = set()
            device = gpu_object[0].device
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
                devices.add(t.device)
            if device.type == "cuda":
                # We have to synchronize before memory registration to assure the object
                # has been created because nixl doesn't guarantee it will.
                for dev in devices:
                    torch.cuda.synchronize(dev)
            with self._nixl_memory_lock:
                nixl_agent = self.get_nixl_agent()
                reg_descs = nixl_agent.register_memory(gpu_object)
                serialized_descs = nixl_agent.get_serialized_descs(reg_descs.trim())
                agent_meta = nixl_agent.get_agent_metadata()
                agent_name = nixl_agent.name
                # If there is any memory deregistered before the registration of this gpu object, we need to tell the receiver to reset the remote agent.
                # Note, we can remove this limit once nixl supports metadata updates.
                reset_remote_agent = (
                    self._memory_deregistered and NIXL_REMOTE_AGENT_CACHE_MAXSIZE > 0
                )
                self._memory_deregistered = False

        else:
            reg_descs, serialized_descs, agent_meta, agent_name, reset_remote_agent = (
                None,
                None,
                None,
                None,
                None,
            )

        ret = NixlTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            nixl_reg_descs=reg_descs,
            nixl_serialized_descs=serialized_descs,
            nixl_agent_meta=agent_meta,
            nixl_agent_name=agent_name,
            reset_remote_agent=reset_remote_agent,
        )
        gpu_object_store.record_managed_meta_nixl(obj_id, ret)
        return ret

    def get_communicator_metadata(
        self,
        src_actor: "ray.actor.ActorHandle",
        dst_actor: "ray.actor.ActorHandle",
        backend: Optional[str] = None,
    ) -> NixlCommunicatorMetadata:
        return NixlCommunicatorMetadata()

    def _update_remote_agent_cache(self, remote_name: str) -> None:
        """Update LRU cache for remote agents; evict least recently used when full."""
        if remote_name in self._remote_agents:
            self._remote_agents.move_to_end(remote_name)
        else:
            if len(self._remote_agents) >= NIXL_REMOTE_AGENT_CACHE_MAXSIZE:
                evicted_agent_name, _ = self._remote_agents.popitem(last=False)
                self.get_nixl_agent().remove_remote_agent(evicted_agent_name)

            self._remote_agents[remote_name] = None

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ) -> List["torch.Tensor"]:
        from ray.experimental.gpu_object_manager.util import (
            create_empty_tensors_from_metadata,
        )

        tensors = create_empty_tensors_from_metadata(tensor_transport_metadata)

        assert isinstance(tensor_transport_metadata, NixlTransportMetadata)
        assert isinstance(communicator_metadata, NixlCommunicatorMetadata)

        nixl_serialized_descs = tensor_transport_metadata.nixl_serialized_descs
        remote_nixl_agent_meta = tensor_transport_metadata.nixl_agent_meta

        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"NIXL transfer aborted for object id: {obj_id}")

        if not tensors:
            return []

        local_descs = None
        remote_name = None
        xfer_handle = None
        try:
            nixl_agent = self.get_nixl_agent()
            remote_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
            local_descs = nixl_agent.register_memory(tensors)

            remote_name = tensor_transport_metadata.nixl_agent_name
            # The sender's nixl agent tells whether we need to reset the remote agent.
            reset_remote_agent = tensor_transport_metadata.reset_remote_agent
            if (
                remote_name is not None
                and remote_name in self._remote_agents
                and reset_remote_agent is not None
                and reset_remote_agent
            ):
                nixl_agent.remove_remote_agent(remote_name)

            remote_name = nixl_agent.add_remote_agent(remote_nixl_agent_meta)
            if isinstance(remote_name, bytes):
                remote_name = remote_name.decode("utf-8")

            if NIXL_REMOTE_AGENT_CACHE_MAXSIZE > 0:
                self._update_remote_agent_cache(remote_name)

            xfer_handle = nixl_agent.initialize_xfer(
                # "UUID" here is just a placeholder, can be any bytes, but without it,
                # nixl will fail to transfer multiple times.
                "READ",
                local_descs.trim(),
                remote_descs,
                remote_name,
                "UUID",
            )

            state = nixl_agent.transfer(xfer_handle)
            if state == "ERR":
                raise RuntimeError("NIXL transfer got to Error state.")
            # Since current nixl does not provide a better way, we need to check the state of
            # the transfer continuously.
            while True:
                state = nixl_agent.check_xfer_state(xfer_handle)
                if state == "ERR":
                    raise RuntimeError("NIXL transfer got to Error state.")
                if state == "PROC":
                    with self._aborted_transfer_obj_ids_lock:
                        if obj_id in self._aborted_transfer_obj_ids:
                            self._aborted_transfer_obj_ids.remove(obj_id)
                            raise RuntimeError(
                                f"NIXL transfer aborted for object id: {obj_id}"
                            )
                    time.sleep(0.001)  # Avoid busy waiting
                elif state == "DONE":
                    break
        except Exception:
            from ray.exceptions import RayDirectTransportError

            raise RayDirectTransportError(
                f"The NIXL recv failed for object id: {obj_id}. The source actor may have died during the transfer. "
                f"The exception thrown from the nixl recv was:\n {traceback.format_exc()}"
            ) from None
        finally:
            # We could raise errors or NIXL could raise errors like NIXL_ERR_REMOTE_DISCONNECT,
            # so doing best effort cleanup.
            with self._aborted_transfer_obj_ids_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            if xfer_handle:
                nixl_agent.release_xfer_handle(xfer_handle)
            if NIXL_REMOTE_AGENT_CACHE_MAXSIZE == 0 and remote_name:
                nixl_agent.remove_remote_agent(remote_name)
            if local_descs:
                nixl_agent.deregister_memory(local_descs)

        return tensors

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: CommunicatorMetadata,
    ):
        raise NotImplementedError(
            "NIXL transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: TensorTransportMetadata
    ):
        from ray._private.worker import global_worker

        assert isinstance(tensor_transport_meta, NixlTransportMetadata)
        gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
        count = gpu_object_store.remove_managed_meta_nixl(obj_id)
        if count == 0:
            descs = tensor_transport_meta.nixl_reg_descs
            if descs is not None:
                with self._nixl_memory_lock:
                    self._memory_deregistered = True
                    self.get_nixl_agent().deregister_memory(descs)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: CommunicatorMetadata,
    ):
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)
