from abc import ABC, abstractmethod

from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    LLMChatResponse,
    LLMCompletionsResponse,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)


class LLMDeployment(ABC):
    """
    This is the common interface between all the llm deployment. All llm deployments
    need to implement an async constructor, an async predict, and check_health method.
    """

    async def __init__(self, llm_config: LLMConfig):
        """
        Constructor takes in an LLMConfig object and start the underlying engine.
        """
        self._llm_config = llm_config

    @abstractmethod
    async def chat(self, request: ChatCompletionRequest) -> LLMChatResponse:
        """
        Inferencing to the engine for chat, and return the response as LLMChatResponse.
        """
        ...

    @abstractmethod
    async def completions(self, request: CompletionRequest) -> LLMCompletionsResponse:
        """
        Inferencing to the engine for completion api, and return the response as LLMCompletionsResponse.
        """
        ...

    @abstractmethod
    async def check_health(self) -> None:
        """
        Check the health of the replica. Does not return anything. Raise error when
        the engine is dead and needs to be restarted.
        """
        ...

    async def llm_config(self) -> LLMConfig:
        return self._llm_config
