from ray.llm._internal.serve.deployments.llm.vllm.vllm_deployment import VLLMDeployment
from ray.llm._internal.serve.deployments.routers.router import LLMModelRouterDeployment

__all__ = ["VLLMDeployment", "LLMModelRouterDeployment"]
