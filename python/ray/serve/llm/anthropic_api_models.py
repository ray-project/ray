from ray.llm._internal.serve.core.configs.anthropic_api_models import (
    AnthropicCountTokensRequest as _AnthropicCountTokensRequest,
    AnthropicCountTokensResponse as _AnthropicCountTokensResponse,
    AnthropicError as _AnthropicError,
    AnthropicErrorResponse as _AnthropicErrorResponse,
    AnthropicMessagesRequest as _AnthropicMessagesRequest,
    AnthropicMessagesResponse as _AnthropicMessagesResponse,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class AnthropicMessagesRequest(_AnthropicMessagesRequest):
    """AnthropicMessagesRequest is the request body for the Messages API.

    This model is compatible with vLLM's Anthropic API models.
    """

    pass


@PublicAPI(stability="stable")
class AnthropicMessagesResponse(_AnthropicMessagesResponse):
    """AnthropicMessagesResponse is the response body for the Messages API.

    This model is compatible with vLLM's Anthropic API models.
    """

    pass


@PublicAPI(stability="stable")
class AnthropicCountTokensRequest(_AnthropicCountTokensRequest):
    """AnthropicCountTokensRequest is the request body for count_tokens.

    This model is compatible with vLLM's Anthropic API models.
    """

    pass


@PublicAPI(stability="stable")
class AnthropicCountTokensResponse(_AnthropicCountTokensResponse):
    """AnthropicCountTokensResponse is the response body for count_tokens.

    This model is compatible with vLLM's Anthropic API models.
    """

    pass


@PublicAPI(stability="stable")
class AnthropicError(_AnthropicError):
    """Anthropic error payload."""

    pass


@PublicAPI(stability="stable")
class AnthropicErrorResponse(_AnthropicErrorResponse):
    """The returned response in case of an Anthropic API error."""

    pass
