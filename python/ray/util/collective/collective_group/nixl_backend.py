from abc import ABCMeta

from nixl._api import nixl_agent, nixl_agent_config
import ray
from ray.util.collective.types import Backend


class NixlBackend(metaclass=ABCMeta):
    def __init__(self):
        agent_config = nixl_agent_config(backends=["UCX"])
        ctx = ray.get_runtime_context()
        actor_id = ctx.get_actor_id()
        self._nixl_agent = nixl_agent(actor_id, agent_config)

    @property
    def nixl_agent(self) -> "nixl_agent":
        return self._nixl_agent

    @classmethod
    def backend(cls):
        return Backend.NIXL

    def recv(self, tensors, nixl_serialized_descs, remote_nixl_agent_meta):
        nixl_agent = self.nixl_agent
        remote_descs = nixl_agent.deserialize_descs(nixl_serialized_descs)
        local_descs = nixl_agent.register_memory(tensors)
        remote_name = nixl_agent.add_remote_agent(remote_nixl_agent_meta)

        xfer_handle = nixl_agent.initialize_xfer(
            "READ", local_descs.trim(), remote_descs, remote_name, b"UUID1"
        )

        state = nixl_agent.transfer(xfer_handle)
        if state == "ERR":
            raise RuntimeError("NIXL transfer got to Error state.")
        while True:
            state = nixl_agent.check_xfer_state(xfer_handle)
            if state == "ERR":
                raise RuntimeError("NIXL transfer got to Error state.")
            elif state == "DONE":
                break

    def get_nixl_metadata(self, tensors):
        nixl_agent = self.nixl_agent
        reg_descs = nixl_agent.register_memory(tensors)
        xfer_descs = reg_descs.trim()
        return (
            nixl_agent.get_serialized_descs(xfer_descs),
            nixl_agent.get_agent_metadata(),
        )
