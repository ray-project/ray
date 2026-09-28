"""Helpers for Anthropic ingress, mirroring vLLM's anthropic api_router."""

from typing import AsyncGenerator, Union

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse, Response, StreamingResponse

from ray.llm._internal.serve.core.configs.anthropic_api_models import (
    AnthropicError,
    AnthropicErrorResponse,
    AnthropicMessagesResponse,
)
from ray.llm._internal.serve.core.configs.openai_api_models import ErrorResponse


def _anthropic_error_response(
    *, status_code: int, error_type: str, message: str
) -> JSONResponse:
    error_response = AnthropicErrorResponse(
        error=AnthropicError(type=error_type, message=message)
    )
    return JSONResponse(
        status_code=status_code,
        content=error_response.model_dump(exclude_none=True),
    )


def translate_error_response(response: ErrorResponse) -> JSONResponse:
    return _anthropic_error_response(
        status_code=response.error.code,
        error_type=response.error.type,
        message=response.error.message,
    )


async def _handle_anthropic_validation_error(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    error_details = exc.errors()[0] if exc.errors() else {"msg": "Invalid request"}
    message = error_details.get("msg", "Unknown validation error")
    location = error_details.get("loc")
    if location:
        message = f"{message} at {location}"

    return _anthropic_error_response(
        status_code=status.HTTP_400_BAD_REQUEST,
        error_type="invalid_request_error",
        message=message,
    )


async def _handle_anthropic_http_error(
    request: Request, exc: HTTPException
) -> JSONResponse:
    # Anthropic error types:
    # https://platform.claude.com/docs/en/api/errors
    if exc.status_code == status.HTTP_404_NOT_FOUND:
        error_type = "not_found_error"
    elif (
        status.HTTP_400_BAD_REQUEST
        <= exc.status_code
        < status.HTTP_500_INTERNAL_SERVER_ERROR
    ):
        error_type = "invalid_request_error"
    else:
        error_type = "api_error"

    return _anthropic_error_response(
        status_code=exc.status_code,
        error_type=error_type,
        message=str(exc.detail),
    )


def add_anthropic_exception_handlers(app: FastAPI) -> None:
    """Install Anthropic-compatible handlers for client-facing errors."""
    app.add_exception_handler(
        RequestValidationError, _handle_anthropic_validation_error
    )
    app.add_exception_handler(HTTPException, _handle_anthropic_http_error)


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
