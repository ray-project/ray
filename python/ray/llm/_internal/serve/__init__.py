from ray.llm._internal.serve.observability import setup_observability
from ray.llm._internal.serve.observability.logging.setup import (
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)
from ray.llm._internal.serve.configs import (
    LLMConfig,
    ModelLoadingConfig,
    LLMServingArgs,
)
from ray.llm._internal.serve.deployments import VLLMDeployment
from ray.llm._internal.serve.deployments import LLMModelRouterDeployment
from ray.llm._internal.serve.builders import build_vllm_deployment, build_openai_app

# Set up observability
disable_vllm_custom_ops_logger_on_cpu_nodes()
setup_observability()


def _worker_process_setup_hook():
    """Noop setup hook used for ENABLE_WORKER_PROCESS_SETUP_HOOK
    (see python/ray/llm/_internal/serve/configs/constants.py)."""
    pass


__all__ = [
    "LLMConfig",
    "ModelLoadingConfig",
    "LLMServingArgs",
    "VLLMDeployment",
    "LLMModelRouterDeployment",
    "build_vllm_deployment",
    "build_openai_app",
]
