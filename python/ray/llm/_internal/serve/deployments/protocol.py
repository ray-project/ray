from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional, Union

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        ErrorResponse,
    )
    from ray.llm._internal.serve.configs.server_models import LLMConfig


class DeploymentProtocol(ABC):
    @classmethod
    @abstractmethod
    def get_deployment_options(cls, **kwargs) -> Dict[str, Any]:
        """Get the default deployment options for the this deployment."""
        pass


class LLMServerProtocol(DeploymentProtocol):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement a sync constructor, an async start method, and check_health method.
    """

    def __init__(self):
        """
        Constructor takes basic setup that doesn't require async operations.
        """

    @abstractmethod
    async def start(self):
        """
        Start the underlying engine. This handles async initialization.
        """
        ...

    @abstractmethod
    async def chat(
        self, request: "ChatCompletionRequest"
    ) -> AsyncGenerator[Union[str, "ChatCompletionResponse", "ErrorResponse"], None]:
        """
        Inferencing to the engine for chat, and return the response.
        """
        ...

    @abstractmethod
    async def completions(
        self, request: "CompletionRequest"
    ) -> AsyncGenerator[
        Union[List[Union[str, "ErrorResponse"]], "CompletionResponse"], None
    ]:
        """
        Inferencing to the engine for completion api, and return the response.
        """
        ...

    @abstractmethod
    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything.
        Raise error when the engine is dead and needs to be restarted.
        """
        ...

    @abstractmethod
    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""

    @abstractmethod
    async def start_profile(self) -> None:
        """Start profiling"""

    @abstractmethod
    async def stop_profile(self) -> None:
        """Stop profiling"""

    # TODO (Kourosh): This does not belong here.
    async def llm_config(self) -> Optional["LLMConfig"]:
        return None
