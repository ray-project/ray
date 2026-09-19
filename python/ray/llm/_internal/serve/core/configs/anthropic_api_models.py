"""This module contains wrapper classes for Anthropic-compatible protocol models.

Supports vLLM as the underlying engine. If vLLM is not installed or fails to
import, an ImportError is raised at import time.
"""

from pydantic import ConfigDict

try:
    from vllm.entrypoints.anthropic.protocol import (
        AnthropicCountTokensRequest as _AnthropicCountTokensRequest,
        AnthropicCountTokensResponse as _AnthropicCountTokensResponse,
        AnthropicError as _AnthropicError,
        AnthropicErrorResponse as _AnthropicErrorResponse,
        AnthropicMessagesRequest as _AnthropicMessagesRequest,
        AnthropicMessagesResponse as _AnthropicMessagesResponse,
    )
    from vllm.entrypoints.anthropic.serving import (
        AnthropicServingMessages as _AnthropicServingMessages,
    )
except ImportError as _vllm_import_error:
    if isinstance(_vllm_import_error, ModuleNotFoundError) and (
        _vllm_import_error.name == "vllm"
        or (_vllm_import_error.name or "").startswith("vllm.")
    ):
        raise ImportError(
            "vLLM is not installed. Anthropic Messages API models require vLLM. "
            "Install with: `pip install ray[llm]`"
        ) from _vllm_import_error
    raise ImportError(
        "vLLM is installed but failed to import. Anthropic Messages API models "
        "require a working vLLM installation. "
        f"Original error: {_vllm_import_error}"
    ) from _vllm_import_error


class AnthropicCountTokensRequest(_AnthropicCountTokensRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class AnthropicCountTokensResponse(_AnthropicCountTokensResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class AnthropicError(_AnthropicError):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class AnthropicErrorResponse(_AnthropicErrorResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class AnthropicMessagesRequest(_AnthropicMessagesRequest):
    model_config = ConfigDict(arbitrary_types_allowed=True)


class AnthropicMessagesResponse(_AnthropicMessagesResponse):
    model_config = ConfigDict(arbitrary_types_allowed=True)


AnthropicServingMessages = _AnthropicServingMessages
