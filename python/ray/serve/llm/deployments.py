
from ray.llm._internal.serve.deployments.routers.router import (
    LLMModelRouterDeployment as _LLMModelRouterDeployment,
)

from ray.llm._internal.serve.deployments.llm.vllm import (
    VLLMDeployment as _VLLMDeployment,
)

from ray.util.annotations import PublicAPI



@PublicAPI(stability="alpha")
class VLLMDeployment(_VLLMDeployment):
    """The LLM deployment implementation to use vllm and the inferencing engine."""

    pass


@PublicAPI(stability="alpha")
class LLMModelRouterDeployment(_LLMModelRouterDeployment):
    """The router deployment to create OpenAI compatible endpoints and route between
    LLM deployments.

    This deployment creates the following endpoints:
      - /v1/chat/completions: Chat interface (OpenAI-style)
      - /v1/completions: Text completion
      - /v1/models: List available models
      - /v1/models/{model}: Model information
    """

    pass

