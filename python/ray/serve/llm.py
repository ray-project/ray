try:
    from ray.llm._internal.serve import (
        LLMConfig as _LLMConfig,
        VLLMDeployment as _VLLMDeployment,
        LLMModelRouterDeployment as _LLMModelRouterDeployment,
        LLMServingArgs,
    )
except ImportError:
    _LLMConfig = object
    _VLLMDeployment = object
    _LLMModelRouterDeployment = object
    LLMServingArgs = object

from ray.serve.deployment import Application
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LLMConfig(_LLMConfig):
    """The configuration for starting an LLM deployment."""

    ...


@PublicAPI(stability="alpha")
class VLLMDeployment(_VLLMDeployment):
    """The LLM deployment implementation to use vllm and the inferencing engine."""

    ...


@PublicAPI(stability="alpha")
class LLMModelRouterDeployment(_LLMModelRouterDeployment):
    """The router deployment to create OpenAI compatible endpoints and route between
    LLM deployments.

    This deployment creates the following endpoints:
      - /v1/chat/completions: Chat interface (ChatGPT-style)
      - /v1/completions: Text completion
      - /v1/models: List available models
      - /v1/models/{model}: Model information
    """

    ...


@PublicAPI(stability="alpha")
def build_vllm_deployment(llm_config: LLMConfig) -> Application:
    """Helper to build a single vllm deployment from the given llm config.

    Args:
        llm_config: The llm config to build vllm deployment.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve import build_vllm_deployment

    return build_vllm_deployment(llm_config=llm_config)


@PublicAPI(stability="alpha")
def build_openai_app(llm_serving_args: LLMServingArgs) -> Application:
    """Helper to build an OpenAI compatible app with the llm deployment setup from
    the given llm serving args. This is the main entry point for users to create a
    Serve application serving LLMs.

    Args:
        llm_serving_args: The list of llm configs or the paths to the llm config to
            build the app.

    Returns:
        The configured Ray Serve Application router.
    """
    from ray.llm._internal.serve import build_openai_app

    return build_openai_app(llm_serving_args=llm_serving_args)
