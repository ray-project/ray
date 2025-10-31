import time
from typing import TYPE_CHECKING, Any, List, Tuple

from nixl._api import nixl_agent, nixl_agent_config

import ray
from ray.util.collective.types import Backend

if TYPE_CHECKING:
    import torch


class NixlBackend:
    """Backend implementation for NIXL tensor transport.

    This class provides functionality for transferring tensors using NIXL. It handles
    initialization of the NIXL agent, receiving tensors, and managing NIXL metadata.
    """

    def __init__(self):
        """Initialize the NIXL backend.

        Creates a NIXL agent with UCX backend.
        """
        agent_config = nixl_agent_config(backends=["UCX"])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        if actor_id is None:
            # If the actor id is None, it means the current process is a driver.
            import uuid

            actor_id = f"RAY-DRIVER-{uuid.uuid4()}"
        self._nixl_agent = nixl_agent(actor_id, agent_config)

    @classmethod
    def backend(cls):
        """Get the backend type.

        Returns:
            Backend.NIXL: The backend type enum value for NIXL.
        """
        return Backend.NIXL

    def recv(
        self,
        tensors: List["torch.Tensor"],
        nixl_serialized_descs: bytes,
        remote_nixl_agent_meta: bytes,
    ):
        """Receive tensors from a remote NIXL agent.

        Args:
            tensors: List of tensors to receive into.
            nixl_serialized_descs: Serialized NIXL descriptors for the remote tensors.
            remote_nixl_agent_meta: Metadata about the remote NIXL agent.

        Raises:
            RuntimeError: If the NIXL transfer enters an error state.
        """
        nixl_agent = self._nixl_agent
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
                time.sleep(0.001)  # Avoid busy waiting
            elif state == "DONE":
                break

        nixl_agent.release_xfer_handle(xfer_handle)
        nixl_agent.deregister_memory(local_descs)
        nixl_agent.remove_remote_agent(remote_name)

    def get_nixl_metadata(
        self, tensors: List["torch.Tensor"]
    ) -> Tuple[Any, bytes, bytes]:
        """Get NIXL metadata for a set of tensors.

        Args:
            tensors: List of tensors to get metadata for.

        Returns:
            tuple: A tuple containing:
                - Serialized NIXL descriptors for the tensors
                - Metadata about this NIXL agent
        """
        nixl_agent = self._nixl_agent
        reg_descs = nixl_agent.register_memory(tensors)
        xfer_descs = reg_descs.trim()
        return (
            reg_descs,
            nixl_agent.get_serialized_descs(xfer_descs),
            nixl_agent.get_agent_metadata(),
        )

    def deregister_memory(self, descs: Any):
        self._nixl_agent.deregister_memory(descs)
