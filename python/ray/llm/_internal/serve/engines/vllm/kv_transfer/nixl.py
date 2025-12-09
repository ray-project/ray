import os

from ray.llm._internal.serve.engines.vllm.kv_transfer.base import (
    BaseConnectorBackend,
)


class NixlConnectorBackend(BaseConnectorBackend):
    def _set_side_channel_port(self):
        from vllm import envs as vllm_envs
        from vllm.utils.network_utils import get_open_port

        if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_PORT"):
            base_port: int = int(
                self.llm_config.experimental_configs.get(
                    "NIXL_SIDE_CHANNEL_PORT_BASE", get_open_port()
                )
            )
            # Use a deterministic rank-based offset (DP rank if set; else replica hash)
            port = base_port + self._compute_port_offset()
            os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] = str(port)

    def _set_side_channel_host(self):
        from vllm import envs as vllm_envs
        from vllm.utils.network_utils import get_ip

        if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_HOST"):
            os.environ["VLLM_NIXL_SIDE_CHANNEL_HOST"] = get_ip()

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

        self._set_side_channel_port()
        self._set_side_channel_host()

        # We need to overwrite the engine_id to make it unique across replicas.
        engine_id = self.kv_transfer_config.get("engine_id", self._get_unique_suffix())
        host = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_HOST
        port = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_PORT
        self.kv_transfer_config["engine_id"] = "-".join([engine_id, host, str(port)])
