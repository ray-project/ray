from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest as _ChatCompletionRequest,
    ChatCompletionResponse as _ChatCompletionResponse,
    ChatCompletionStreamResponse as _ChatCompletionStreamResponse,
    CompletionRequest as _CompletionRequest,
    CompletionResponse as _CompletionResponse,
    CompletionStreamResponse as _CompletionStreamResponse,
    EmbeddingRequest as _EmbeddingRequest,
    EmbeddingResponse as _EmbeddingResponse,
    ErrorResponse as _ErrorResponse,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class ChatCompletionRequest(_ChatCompletionRequest):
    """ChatCompletionRequest is the request body for the chat completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class CompletionRequest(_CompletionRequest):
    """CompletionRequest is the request body for the completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class ChatCompletionStreamResponse(_ChatCompletionStreamResponse):
    """ChatCompletionStreamResponse is the response body for the chat completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class ChatCompletionResponse(_ChatCompletionResponse):
    """ChatCompletionResponse is the response body for the chat completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class CompletionStreamResponse(_CompletionStreamResponse):
    """CompletionStreamResponse is the response body for the completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class CompletionResponse(_CompletionResponse):
    """CompletionResponse is the response body for the completion API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class EmbeddingRequest(_EmbeddingRequest):
    """EmbeddingRequest is the request body for the embedding API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class EmbeddingResponse(_EmbeddingResponse):
    """EmbeddingResponse is the response body for the embedding API.

    This model is compatible with vLLM's OpenAI API models.
    """

    pass


@PublicAPI(stability="alpha")
class ErrorResponse(_ErrorResponse):
    """The returned response in case of an error."""

    pass
