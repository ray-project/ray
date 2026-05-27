import os

import ray
from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)

_DEFAULT_SIDE_CHANNEL_PORT_START = 20000


class NixlConnectorBackend(BaseConnectorBackend):
    def _set_side_channel_port(self):
        from vllm import envs as vllm_envs

        if vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_PORT"):
            return

        # _compute_port_offset() is deterministic per replica (DP rank if set,
        # else Serve replica rank * num_devices), so replicas within one
        # deployment land on non-colliding ports. Operators running multiple
        # PD deployments on a single node should set NIXL_SIDE_CHANNEL_PORT_BASE
        # to non-overlapping ranges per deployment.
        base_port = int(
            self.llm_config.experimental_configs.get(
                "NIXL_SIDE_CHANNEL_PORT_BASE", _DEFAULT_SIDE_CHANNEL_PORT_START
            )
        )
        port = base_port + self._compute_port_offset()
        os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] = str(port)

    def _set_side_channel_host(self):
        from vllm import envs as vllm_envs

        if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_HOST"):
            # Use Ray's node IP (internal/cluster IP) instead of vLLM's
            # get_ip() which can return external/public IPs on hostNetwork
            # pods, causing cross-node NIXL handshakes to fail.
            os.environ["VLLM_NIXL_SIDE_CHANNEL_HOST"] = ray.util.get_node_ip_address()

    def setup(self) -> None:
        """Initialize the NIXL connector backend.

        This method sets up the NIXL (NVIDIA Inference Xfer Library) connector by:
        1. Verifying that the required vLLM environment variables are supported
        2. Configuring the side channel port and host if not already set
        3. Creating a unique engine ID across replicas

        The side channel is used for KV cache transfer between vLLM instances.

        Raises:
            ValueError: If the current vLLM version doesn't support the required
                       NIXL environment variables.
        """
        from vllm import envs as vllm_envs

        if (
            "VLLM_NIXL_SIDE_CHANNEL_PORT" not in vllm_envs.environment_variables
            or "VLLM_NIXL_SIDE_CHANNEL_HOST" not in vllm_envs.environment_variables
        ):
            raise ValueError(
                "This vLLM version does not support VLLM_NIXL_SIDE_CHANNEL_PORT"
                "or VLLM_NIXL_SIDE_CHANNEL_HOST environment variable. It's likely"
                "that you are using an older version of vLLM."
            )

        self._set_side_channel_host()
        self._set_side_channel_port()

        # We need to overwrite the engine_id to make it unique across replicas.
        engine_id = self.kv_transfer_config.get("engine_id", self._get_unique_suffix())
        host = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_HOST
        port = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_PORT
        self.kv_transfer_config["engine_id"] = "-".join([engine_id, host, str(port)])
