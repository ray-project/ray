"""Shared helpers for OpenAI ingress, reused by the P/D direct-streaming path."""

import json
from typing import AsyncGenerator, List, Tuple, TypeVar, Union

from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionStreamResponse,
    CompletionResponse,
    CompletionStreamResponse,
    TranscriptionResponse,
    TranscriptionStreamResponse,
)

T = TypeVar("T")

NON_STREAMING_RESPONSE_TYPES = (
    ChatCompletionResponse,
    CompletionResponse,
    TranscriptionResponse,
)

StreamResponseType = Union[
    ChatCompletionStreamResponse, CompletionStreamResponse, TranscriptionStreamResponse
]
BatchedStreamResponseType = List[StreamResponseType]


def _sanitize_chat_completion_request(
    request: ChatCompletionRequest,
) -> ChatCompletionRequest:
    """Sanitize ChatCompletionRequest to fix Pydantic ValidatorIterator serialization issue.

    This addresses a known Pydantic bug where fields typed as ``Iterable[...]``
    on OpenAI message TypedDicts (notably ``content`` on every message variant
    and ``tool_calls`` on assistant messages) become ValidatorIterator objects
    that cannot be pickled for Ray remote calls.

    Workaround logic adapted from vLLM (credits: @gcalmettes):
    - vLLM PR: https://github.com/vllm-project/vllm/pull/9951
    - Pydantic Issue: https://github.com/pydantic/pydantic/issues/9467
    - Related Issue: https://github.com/pydantic/pydantic/issues/9541
    - Official Workaround: https://github.com/pydantic/pydantic/issues/9467#issuecomment-2442097291

    Note: still reproducible on Pydantic 2.12 for the ``Iterable[...]`` arm of
    a ``Union``, so this sanitizer is required regardless of Pydantic version.
    """
    for i, message in enumerate(request.messages):
        # SGLang messages are Pydantic BaseModels (no .get()); convert to dicts
        # so the same logic works for both vLLM (TypedDict) and SGLang.
        if not isinstance(message, dict):
            request.messages[i] = message = message.model_dump()

        # `content` is typed `Union[str, Iterable[ContentPart], None]` on every
        # OpenAI message variant. When the iterable arm matches, Pydantic stores
        # a non-picklable ValidatorIterator. Materialize it for any role.
        content_val = message.get("content")
        if content_val is not None and not isinstance(content_val, str):
            try:
                message["content"] = list(content_val)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    "Validating message `content` raised an error. Please "
                    "ensure `content` is a string, None, or an iterable of "
                    "content parts."
                ) from e

        if message.get("role") == "assistant":
            tool_calls_val = message.get("tool_calls")
            if tool_calls_val is not None:
                try:
                    message["tool_calls"] = list(tool_calls_val)
                except (TypeError, ValueError) as e:
                    raise ValueError(
                        "Validating messages' `tool_calls` raised an error. "
                        "Please ensure `tool_calls` are iterable of tool calls."
                    ) from e

    return request


def _apply_openai_json_format(
    response: Union[StreamResponseType, BatchedStreamResponseType],
) -> str:
    """Converts the stream response to OpenAI format.

    Each model response is converted to the string:
        data: <response-json1>\n\n

    The converted strings are concatenated and returned:
        data: <response-json1>\n\ndata: <response-json2>\n\n...
    """
    if isinstance(response, list):
        first_response = next(iter(response))
        if isinstance(first_response, str):
            return "".join(response)
        if isinstance(first_response, dict):
            return "".join(f"data: {json.dumps(r)}\n\n" for r in response)
        if hasattr(first_response, "model_dump_json"):
            return "".join(f"data: {r.model_dump_json()}\n\n" for r in response)
        raise ValueError(
            f"Unexpected response type: {type(first_response)}, {first_response=}"
        )
    if hasattr(response, "model_dump_json"):
        return f"data: {response.model_dump_json()}\n\n"
    if isinstance(response, str):
        return response
    raise ValueError(f"Unexpected response type: {type(response)}, {response=}")


async def _peek_at_generator(
    gen: AsyncGenerator[T, None],
) -> Tuple[T, AsyncGenerator[T, None]]:
    # Peek at the first element
    first_item = await gen.__anext__()

    # Create a new generator that yields the peeked item first
    async def new_generator() -> AsyncGenerator[T, None]:
        yield first_item
        async for item in gen:
            yield item

    return first_item, new_generator()


async def _openai_json_wrapper(
    generator: AsyncGenerator[
        Union[StreamResponseType, BatchedStreamResponseType], None
    ],
) -> AsyncGenerator[str, None]:
    """Wrapper that converts stream responses into OpenAI JSON strings.

    Args:
        generator: an async generator that yields either individual stream responses
            (StreamResponseType) or batches of stream responses (BatchedStreamResponseType).
            Each response is converted into OpenAI JSON format and streamed to the client.
            For batched responses, the items are concatenated together as a single string.

    Yields:
        String chunks in OpenAI SSE format: "data: {json}\n\n", with a final
        "data: [DONE]\n\n" to indicate completion. If the upstream generator
        already yields a "data: [DONE]" sentinel, it is not duplicated.
    """
    done_sent = False
    async for response in generator:
        packet = _apply_openai_json_format(response)
        if packet.strip().endswith("data: [DONE]"):
            done_sent = True
        yield packet

    if not done_sent:
        yield "data: [DONE]\n\n"
