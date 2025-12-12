import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, List, Optional

import ray
from ray.experimental.gpu_object_manager.tensor_transport_manager import (
    TensorTransportManager,
)
from ray.experimental.gpu_object_manager.types import (
    CommunicatorMetadata,
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
    """

    nixl_reg_descs: Optional[Any] = None
    nixl_serialized_descs: Optional[bytes] = None
    nixl_agent_meta: Optional[bytes] = None

    __eq__ = object.__eq__
    __hash__ = object.__hash__


class NixlTensorTransport(TensorTransportManager):
    def __init__(self, tensor_transport_backend: str):
        # This is lazily initialized because it requires NIXL to actually be installed and we want to allow an owner that is just coordinating to not need to have NIXL installed.
        self._nixl_agent = None
        self._aborted_transfer_obj_ids = set()
        self._aborted_transfer_obj_ids_lock = threading.Lock()

    @property
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
            nixl_agent = self.get_nixl_agent()
            reg_descs = nixl_agent.register_memory(gpu_object)
            serialized_descs = nixl_agent.get_serialized_descs(reg_descs.trim())
            agent_meta = nixl_agent.get_agent_metadata()
            # We assume all tensors in one GPU object have the same device type.
            device = gpu_object[0].device
            for t in gpu_object:
                if t.device.type != device.type:
                    raise ValueError(
                        "All tensors in an RDT object must have the same device type."
                    )
                tensor_meta.append((t.shape, t.dtype))
        else:
            reg_descs, serialized_descs, agent_meta = None, None, None

        ret = NixlTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=device,
            nixl_reg_descs=reg_descs,
            nixl_serialized_descs=serialized_descs,
            nixl_agent_meta=agent_meta,
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

    def recv_multiple_tensors(
        self,
        tensors,
        obj_id: str,
        tensor_transport_metadata: NixlTransportMetadata,
        communicator_metadata: NixlCommunicatorMetadata,
    ):
        if not tensors:
            return

        assert isinstance(
            tensor_transport_metadata, NixlTransportMetadata
        ), "metadata must be a NixlTransportMetadata object for NIXL transport"
        assert isinstance(
            communicator_metadata, NixlCommunicatorMetadata
        ), "metadata must be a NixlCommunicatorMetadata object for NIXL transport"

        nixl_serialized_descs = tensor_transport_metadata.nixl_serialized_descs
        remote_nixl_agent_meta = tensor_transport_metadata.nixl_agent_meta

        with self._aborted_transfer_obj_ids_lock:
            if obj_id in self._aborted_transfer_obj_ids:
                self._aborted_transfer_obj_ids.remove(obj_id)
                raise RuntimeError(f"NIXL transfer aborted for object id: {obj_id}")

        local_descs = None
        remote_name = None
        xfer_handle = None
        try:
            nixl_agent = self.get_nixl_agent()
            remote_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
            local_descs = nixl_agent.register_memory(tensors)
            remote_name = nixl_agent.add_remote_agent(remote_nixl_agent_meta)

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
        finally:
            # We could raise errors or NIXL could raise errors like NIXL_ERR_REMOTE_DISCONNECT,
            # so doing best effort cleanup.
            with self._aborted_transfer_obj_ids_lock:
                self._aborted_transfer_obj_ids.discard(obj_id)
            if xfer_handle:
                nixl_agent.release_xfer_handle(xfer_handle)
            if remote_name:
                nixl_agent.remove_remote_agent(remote_name)
            if local_descs:
                nixl_agent.deregister_memory(local_descs)

    def send_multiple_tensors(
        self,
        tensors: List["torch.Tensor"],
        tensor_transport_metadata: NixlTransportMetadata,
        communicator_metadata: NixlCommunicatorMetadata,
    ):
        raise NotImplementedError(
            "NIXL transport does not support send_multiple_tensors, since it is a one-sided transport."
        )

    def garbage_collect(
        self, obj_id: str, tensor_transport_meta: NixlTransportMetadata
    ):
        from ray._private.worker import global_worker

        gpu_object_store = global_worker.gpu_object_manager.gpu_object_store
        count = gpu_object_store.remove_managed_meta_nixl(obj_id)
        if count == 0:
            descs = tensor_transport_meta.nixl_reg_descs
            if descs is not None:
                self.get_nixl_agent().deregister_memory(descs)

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: NixlCommunicatorMetadata,
    ):
        with self._aborted_transfer_obj_ids_lock:
            self._aborted_transfer_obj_ids.add(obj_id)
