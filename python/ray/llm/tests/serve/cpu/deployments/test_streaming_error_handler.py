import sys

import pytest

import ray
from ray.llm._internal.serve.configs.error_handling import (
    InputTooLong,
    ValidationError,
)
from ray.llm._internal.serve.configs.prompt_formats import Prompt
from ray.llm._internal.serve.configs.server_models import LLMRawResponse
from ray.llm._internal.serve.deployments.utils.error_handling_utils import (
    StreamingErrorHandler,
)


async def fake_generator_internal_error():
    for _ in range(4):
        yield LLMRawResponse(num_generated_tokens=1, generated_text="abcd")
    raise RuntimeError("error")


async def fake_generator_pydantic_validation_error():
    for _ in range(4):
        yield LLMRawResponse(num_generated_tokens=1, generated_text="abcd")
    Prompt(prompt=None)


async def fake_generator_validation_error():
    for _ in range(4):
        yield LLMRawResponse(num_generated_tokens=1, generated_text="abcd")
    raise ValidationError("error")


async def fake_generator_prompt_too_long():
    for _ in range(4):
        yield LLMRawResponse(num_generated_tokens=1, generated_text="abcd")
    raise InputTooLong(2, 1).exception


@pytest.fixture
def handler():
    error_handler = StreamingErrorHandler()
    request_id = "rid123"
    ray.serve.context._serve_request_context.set(
        ray.serve.context._RequestContext(**{"request_id": request_id})
    )
    return error_handler, request_id


@pytest.mark.asyncio
async def test_streaming_error_handler_internal_server_error(handler):
    error_handler, request_id = handler
    generator = fake_generator_internal_error()

    async for response in error_handler.handle_failure("model", generator):
        last_response = response
    assert (
        last_response.error.message
        == f"Internal Server Error (Request ID: {request_id})"
    )
    assert (
        last_response.error.internal_message
        == f"Internal Server Error (Request ID: {request_id})"
    )
    assert last_response.error.type == "InternalServerError"
    assert last_response.error.code == 500


@pytest.mark.asyncio
async def test_streaming_error_handler_pydantic_validation_error(handler):
    error_handler, request_id = handler
    generator = fake_generator_pydantic_validation_error()

    async for response in error_handler.handle_failure("model", generator):
        last_response = response
    assert last_response.error.message.startswith(
        "prompt.list[function-after[check_fields(), Message]]\n  Input should be a valid list [type=list_type, input_value=None, input_type=NoneType]"
    ) and last_response.error.message.endswith(f"(Request ID: {request_id})")
    assert last_response.error.internal_message.startswith(
        "prompt.list[function-after[check_fields(), Message]]\n  Input should be a valid list [type=list_type, input_value=None, input_type=NoneType]"
    ) and last_response.error.internal_message.endswith(f"(Request ID: {request_id})")
    assert last_response.error.type == "ValidationError"
    assert last_response.error.code == 400


@pytest.mark.asyncio
async def test_streaming_error_handler_validation_error(handler):
    error_handler, request_id = handler
    generator = fake_generator_validation_error()

    async for response in error_handler.handle_failure("model", generator):
        last_response = response
    assert (
        last_response.error.message
        == f"ray.llm._internal.serve.configs.error_handling.ValidationError: error (Request ID: {request_id})"
    )
    assert (
        last_response.error.internal_message
        == f"ray.llm._internal.serve.configs.error_handling.ValidationError: error (Request ID: {request_id})"
    )
    assert last_response.error.type == "ValidationError"
    assert last_response.error.code == 400


@pytest.mark.asyncio
async def test_streaming_error_handler_prompt_too_long(handler):
    error_handler, request_id = handler
    generator = fake_generator_prompt_too_long()

    async for response in error_handler.handle_failure("model", generator):
        last_response = response
    assert (
        last_response.error.message
        == f"ray.llm._internal.serve.configs.error_handling.PromptTooLongError: Input too long. Received 2 tokens, but the maximum input length is 1 tokens. (Request ID: {request_id})"
    )
    assert (
        last_response.error.internal_message
        == f"ray.llm._internal.serve.configs.error_handling.PromptTooLongError: Input too long. Received 2 tokens, but the maximum input length is 1 tokens. (Request ID: {request_id})"
    )
    assert last_response.error.type == "PromptTooLongError"
    assert last_response.error.code == 400


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
