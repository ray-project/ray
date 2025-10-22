from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Protocol,
    Union,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
    from ray.llm._internal.serve.core.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        ErrorResponse,
    )


class DeploymentProtocol(Protocol):
    @classmethod
    def get_deployment_options(cls, **kwargs) -> Dict[str, Any]:
        """Get the default deployment options for the this deployment."""


class LLMServerProtocol(DeploymentProtocol):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement a sync constructor, an async start method, and check_health method.
    """

    def __init__(self):
        """
        Constructor takes basic setup that doesn't require async operations.
        """

    async def start(self) -> None:
        """
        Start the underlying engine. This handles async initialization.
        """

    async def chat(
        self, request: "ChatCompletionRequest"
    ) -> AsyncGenerator[Union[str, "ChatCompletionResponse", "ErrorResponse"], None]:
        """
        Inferencing to the engine for chat, and return the response.
        """

    async def completions(
        self, request: "CompletionRequest"
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """
        Inferencing to the engine for completion api, and return the response.
        """

    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything.
        Raise error when the engine is dead and needs to be restarted.
        """

    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""

    async def start_profile(self) -> None:
        """Start profiling"""

    async def stop_profile(self) -> None:
        """Stop profiling"""

    # TODO (Kourosh): This does not belong here.
    async def llm_config(self) -> Optional["LLMConfig"]:
        """Get the LLM config"""
