"""Builder for SGLang Wide-EP deployments."""

from typing import Optional

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.serving_patterns.sglang.sglang_dp_server import (
    SGLangDPServer,
)
from ray.serve.deployment import Application


def build_sglang_dp_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
    deployment_cls: Optional[type] = None,
) -> Application:
    """Build a SGLang Wide-EP deployment.

    Args:
        llm_config: The LLM configuration with engine_kwargs containing
            moe_dp_size, enable_dp_attention, tp_size, pp_size, etc.
        name_prefix: The prefix to add to the deployment name.
        bind_kwargs: Optional extra kwargs to pass to the deployment constructor.
        override_serve_options: Optional serve options to override defaults.
        deployment_cls: Optional deployment class. Defaults to SGLangDPServer.

    Returns:
        The Ray Serve Application for the SGLang Wide-EP deployment.
    """
    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        bind_kwargs=bind_kwargs,
        override_serve_options=override_serve_options,
        deployment_cls=deployment_cls or SGLangDPServer,
    )
