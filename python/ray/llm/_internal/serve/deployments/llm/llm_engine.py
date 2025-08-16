import abc
from typing import TYPE_CHECKING, AsyncGenerator, Union

from ray.llm._internal.serve.configs.server_models import (
    DiskMultiplexConfig,
    LLMConfig,
)

if TYPE_CHECKING:
    from ray.llm._internal.serve.configs.openai_api_models import (
        ChatCompletionRequest,
        ChatCompletionResponse,
        CompletionRequest,
        CompletionResponse,
        EmbeddingRequest,
        EmbeddingResponse,
        ErrorResponse,
    )


class LLMEngine(abc.ABC):
    """Base protocol class for all LLM engines."""

    @abc.abstractmethod
    def __init__(self, llm_config: LLMConfig):
        """Initialize the engine with the llm config"""
        pass

    @abc.abstractmethod
    async def start(self):
        """Start the engine"""
        pass

    @abc.abstractmethod
    async def resolve_lora(self, lora_model: DiskMultiplexConfig):
        """Mounts the LoRA model on the engine, given the local disk path."""
        pass

    @abc.abstractmethod
    async def reset_prefix_cache(self) -> None:
        """Reset the prefix cache of the underlying engine"""

    @abc.abstractmethod
    async def chat(
        self, request: "ChatCompletionRequest"
    ) -> AsyncGenerator[Union[str, "ChatCompletionResponse", "ErrorResponse"], None]:
        """Run a ChatCompletion with the engine.

        To implement this method, you need to take a openAI compatible chat request, internally cast it to the target engine request type, and then call the engine's chat method.

        This method is an async generator, so it yields chunks of response and when it is done, it returns None. We have the following convention:

        - In case of streaming, yield a string representing data: <json_str>\n\n for each chunk. This should be already openAI compatible, so the higher level can just yield it to the client.
        - In case of non-streaming, yield a single object of type ChatCompletionResponse.
        - In case of error, yield a single object of type ErrorResponse.

        Args:
            request: The chat completion request.

        Yields:
            Union[str, ChatCompletionResponse, ErrorResponse]: A string representing a chunk of the response, a ChatCompletionResponse object, or an ErrorResponse object.

        Returns:
            None when the generator is done.
        """
        pass

    @abc.abstractmethod
    async def completions(
        self, request: "CompletionRequest"
    ) -> AsyncGenerator[Union[str, "CompletionResponse", "ErrorResponse"], None]:
        """Run a Completion with the engine.

        Similar to chat, this method is an async generator, so it yields chunks
        of response and when it is done, it returns None. We have the following
        convention:

        * In case of streaming, yield a string representing data:
        <json_str>\n\n for each chunk. This should be already openAI compatible
        with completion response format, so the higher level can just yield it
        directly to the client.
        * In case of non-streaming, yield a single object of type
        CompletionResponse.
        * In case of error, yield a single object of type ErrorResponse.

        Args:
            request: The completion request.

        Yields:
            Union[str, CompletionResponse, ErrorResponse]: A string
            representing a chunk of the response, a CompletionResponse object,
            or an ErrorResponse object.

        Returns:
            None when the generator is done.
        """
        pass

    @abc.abstractmethod
    async def embeddings(
        self, request: "EmbeddingRequest"
    ) -> AsyncGenerator[Union["EmbeddingResponse", "ErrorResponse"], None]:
        """Run an Embedding with the engine.

        This method is different from chat and completion in that it does not
        have streaming, but still it is an async generator that yields response
        objects and when it is done, it returns None. We have the following
        convention:

        * yield a single object of type EmbeddingResponse.
        * For errors, yield a single object of type ErrorResponse.

        Args:
            request: The embedding request.

        Returns:
            An async generator that yields EmbeddingResponse objects or ErrorResponse objects, and returns None when the generator is done.
        """
        pass

    async def check_health(self) -> None:
        """Check the health of the engine.

        Does not return anything. Raise error when the engine is dead and needs
        to be restarted.
        """
        return

    ##############################################################
    # Optional methods
    # These methods will be implemented in the future to allow
    # more granular life-cycle management of the engine.
    # e.g. in usecases like RL training, we need to put the engine
    # to sleep during training and wake up during rollouts.
    ##############################################################

    async def sleep(self):
        """Puts the engine to sleep"""
        pass

    async def wakeup(self):
        """Wakes up the engine"""
        pass

    def shutdown(self):
        """Shuts down the engine"""
        pass
