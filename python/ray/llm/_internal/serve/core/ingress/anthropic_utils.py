"""Helpers for Anthropic ingress, mirroring vLLM's anthropic api_router."""

from typing import AsyncGenerator, Union

from starlette.responses import JSONResponse, Response, StreamingResponse

from ray.llm._internal.serve.core.configs.anthropic_api_models import (
    AnthropicError,
    AnthropicErrorResponse,
    AnthropicMessagesResponse,
)
from ray.llm._internal.serve.core.configs.openai_api_models import ErrorResponse


def translate_error_response(response: ErrorResponse) -> JSONResponse:
    anthropic_error = AnthropicErrorResponse(
        error=AnthropicError(
            type=response.error.type,
            message=response.error.message,
        )
    )
    return JSONResponse(
        status_code=response.error.code,
        content=anthropic_error.model_dump(exclude_none=True),
    )


def anthropic_messages_http_response(
    result: Union[
        ErrorResponse,
        AnthropicMessagesResponse,
        AsyncGenerator[str, None],
    ],
) -> Response:
    if isinstance(result, ErrorResponse):
        return translate_error_response(result)

    if isinstance(result, AnthropicMessagesResponse):
        return JSONResponse(content=result.model_dump(exclude_none=True))

    return StreamingResponse(content=result, media_type="text/event-stream")
