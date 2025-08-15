import os

from ray.llm._internal.serve.deployments.llm.vllm.kv_transfer_backends.base import (
    BaseConnectorBackend,
)


class NixlConnectorBackend(BaseConnectorBackend):
    def setup(self) -> None:
        """Initialize the NIXL connector backend.

        This method sets up the NIXL (Network Interface for eXtended LLM) connector by:
        1. Verifying that the required vLLM environment variables are supported
        2. Configuring the side channel port and host if not already set
        3. Creating a unique engine ID across replicas

        The side channel is used for KV cache transfer between vLLM instances.

        Raises:
            ValueError: If the current vLLM version doesn't support the required
                       NIXL environment variables.
        """
        from vllm import envs as vllm_envs, utils as vllm_utils

        if (
            "VLLM_NIXL_SIDE_CHANNEL_PORT" not in vllm_envs.environment_variables
            or "VLLM_NIXL_SIDE_CHANNEL_HOST" not in vllm_envs.environment_variables
        ):
            raise ValueError(
                "This vLLM version does not support VLLM_NIXL_SIDE_CHANNEL_PORT"
                "or VLLM_NIXL_SIDE_CHANNEL_HOST environment variable. It's likely"
                "that you are using an older version of vLLM."
            )

        if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_PORT"):
            port: int = vllm_utils.get_open_port()
            os.environ["VLLM_NIXL_SIDE_CHANNEL_PORT"] = str(port)
        if not vllm_envs.is_set("VLLM_NIXL_SIDE_CHANNEL_HOST"):
            os.environ["VLLM_NIXL_SIDE_CHANNEL_HOST"] = vllm_utils.get_ip()

        # We need to overwrite the engine_id to make it unique across replicas.
        engine_id = self.kv_transfer_config.get("engine_id", self._get_unique_suffix())
        host = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_HOST
        port = vllm_envs.VLLM_NIXL_SIDE_CHANNEL_PORT
        self.kv_transfer_config["engine_id"] = "-".join([engine_id, host, str(port)])
