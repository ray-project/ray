"""Builder for SGLang WideEP deployment."""

from typing import Optional

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.core.server.builder import build_llm_deployment
from ray.llm._internal.serve.observability.logging import get_logger
from ray.llm._internal.serve.serving_patterns.sglang.sglang_wideep_server import (
    SGLangWideEPServer,
)
from ray.serve.deployment import Application

logger = get_logger(__name__)


def build_sglang_wideep_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    bind_kwargs: Optional[dict] = None,
    override_serve_options: Optional[dict] = None,
) -> Application:
    """Build a SGLang WideEP LLM deployment.

    WideEP is enabled via moe_a2a_backend="deepep" which automatically sets
    ep_size = tp_size. Uses gang scheduling with gang_size=1 (single engine
    per gang, internally manages all DP/TP/EP ranks).

    Args:
        llm_config: The LLM configuration. Must include moe_a2a_backend="deepep"
            in engine_kwargs to enable WideEP.
        name_prefix: The prefix to add to the deployment name.
        bind_kwargs: Optional extra kwargs to pass to the deployment constructor.
        override_serve_options: Optional serve options to override defaults.

    Returns:
        The Ray Serve Application for the SGLang WideEP deployment.
    """
    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        bind_kwargs=bind_kwargs,
        override_serve_options=override_serve_options,
        deployment_cls=SGLangWideEPServer,
    )
