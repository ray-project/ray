
from typing import TYPE_CHECKING

from ray.util.annotations import PublicAPI

if TYPE_CHECKING:
    from ray.serve.deployment import Application
    from ray.serve.llm.configs import LLMConfig, LLMServingArgs


@PublicAPI(stability="alpha")
def build_vllm_deployment(llm_config: "LLMConfig") -> "Application":
    """Helper to build a single vllm deployment from the given llm config.

    Args:
        llm_config: The llm config to build vllm deployment.

    Returns:
        The configured Ray Serve Application for vllm deployment.
    """
    from ray.llm._internal.serve import build_vllm_deployment

    return build_vllm_deployment(llm_config=llm_config)


@PublicAPI(stability="alpha")
def build_openai_app(llm_serving_args: "LLMServingArgs") -> "Application":
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
