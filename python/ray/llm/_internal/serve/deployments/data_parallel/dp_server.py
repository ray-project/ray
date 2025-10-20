import logging
from typing import Optional

from ray import serve
from ray.llm._internal.serve.configs.server_models import LLMConfig
from ray.llm._internal.serve.deployments.llm.builder_llm_server import (
    build_llm_deployment,
)
from ray.llm._internal.serve.deployments.routers.router import (
    OpenAiIngress,
    make_fastapi_ingress,
)
from ray.serve.deployment import Application

logger = logging.getLogger(__name__)


def build_dp_deployment(
    llm_config: LLMConfig,
    *,
    name_prefix: Optional[str] = None,
    override_serve_options: Optional[dict] = None,
) -> Application:
    """Build a data parallel LLM deployment.

    Each Serve replica represents one complete DP group. vLLM will spawn
    data_parallel_size worker actors internally using Ray's distributed executor.

    Args:
        llm_config: LLM configuration with engine_kwargs.data_parallel_size set.
        name_prefix: Optional prefix for the deployment name.
        override_serve_options: Optional overrides for Serve deployment options.

    Returns:
        A Serve Application for the DP deployment.

    Example:
        # Single Serve replica with DP group of 8 workers
        llm_config = LLMConfig(engine_kwargs={"data_parallel_size": 8})
        app = build_dp_deployment(llm_config)  # Creates 1 Serve replica (8 workers internally)

        # Two Serve replicas, each with DP group of 8 workers (2x DPEP8)
        llm_config = LLMConfig(
            engine_kwargs={"data_parallel_size": 8},
            deployment_config={"num_replicas": 2}  # 2 independent DP groups
        )
        app = build_dp_deployment(llm_config)
    """
    dp_size = llm_config.engine_kwargs.get("data_parallel_size", 1)
    if dp_size == 1:
        raise ValueError(
            "data_parallel_size should be greater than 1 for DP deployment."
        )

    return build_llm_deployment(
        llm_config,
        name_prefix=name_prefix,
        override_serve_options=override_serve_options,
    )


def build_openai_dp_app(llm_config: LLMConfig) -> Application:

    dp_deployment = build_dp_deployment(llm_config)
    ingress_options = OpenAiIngress.get_deployment_options([llm_config])

    ingress_cls = make_fastapi_ingress(OpenAiIngress)
    ingress_app = serve.deployment(ingress_cls, **ingress_options).bind(
        llm_deployments=[dp_deployment]
    )

    return ingress_app
