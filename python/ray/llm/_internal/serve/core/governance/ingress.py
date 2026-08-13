from typing import Any, AsyncGenerator, Dict, List, Optional, Union

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from ray.llm._internal.serve.constants import DEFAULT_LLM_ROUTER_HTTP_TIMEOUT
from ray.llm._internal.serve.core.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    ErrorResponse,
    OpenAIHTTPException,
    TranscriptionRequest,
)
from ray.llm._internal.serve.core.governance.chain import MiddlewareChain
from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
    blocked_response_to_http,
    build_request_context,
    usage_to_dict,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware
from ray.llm._internal.serve.core.ingress.ingress import (
    OpenAiIngress,
    router_request_timeout,
)
from ray.llm._internal.serve.core.ingress.utils import (
    NON_STREAMING_RESPONSE_TYPES,
    _openai_json_wrapper,
    _peek_at_generator,
)


def _update_last_usage(last_usage: Dict[str, Any], item: Any) -> None:
    if isinstance(item, list):
        for chunk in item:
            usage = usage_to_dict(chunk)
            if usage:
                last_usage.clear()
                last_usage.update(usage)
    else:
        usage = usage_to_dict(item)
        if usage:
            last_usage.clear()
            last_usage.update(usage)


async def _stream_with_completion_hook(
    chain: MiddlewareChain,
    context: RequestContext,
    gen: AsyncGenerator,
    substitute_first: Any = None,
) -> AsyncGenerator:
    """Yield stream chunks from a peeked generator, then run on_inference_complete.

    ``gen`` comes from ``_peek_at_generator`` and already replays the first item.
    When ``substitute_first`` is set, that value is yielded instead of the
    replayed first item (avoids duplicate first chunks).
    """
    last_usage: Dict[str, Any] = {}
    try:
        first = True
        async for item in gen:
            if first:
                first = False
                item_to_yield = (
                    substitute_first if substitute_first is not None else item
                )
            else:
                item_to_yield = item
            _update_last_usage(last_usage, item_to_yield)
            yield item_to_yield
    finally:
        await chain.on_inference_complete(last_usage, context)


class GovernanceIngress(OpenAiIngress):
    def __init__(
        self,
        *args,
        middlewares: Optional[List[LLMMiddleware]] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._chain = MiddlewareChain(middlewares or [])

    async def _process_llm_request(
        self,
        body: Union[CompletionRequest, ChatCompletionRequest, TranscriptionRequest],
        call_method: str,
        raw_request: Optional[Request] = None,
    ) -> Response:
        context = build_request_context(body, raw_request)

        before_result = await self._chain.before_inference(body, context)
        if isinstance(before_result, BlockedResponse):
            return blocked_response_to_http(before_result)
        body = before_result

        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            gen = self._get_response(
                body=body, call_method=call_method, raw_request=raw_request
            )
            initial_response, gen = await _peek_at_generator(gen)

            if isinstance(initial_response, list):
                first_chunk = initial_response[0]
            else:
                first_chunk = initial_response

            if isinstance(first_chunk, ErrorResponse):
                raise OpenAIHTTPException(
                    message=first_chunk.error.message,
                    status_code=first_chunk.error.code,
                    type=first_chunk.error.type,
                )

            if isinstance(first_chunk, NON_STREAMING_RESPONSE_TYPES):
                response = await self._chain.after_inference(body, first_chunk, context)
                await self._chain.on_inference_complete(
                    usage_to_dict(response), context
                )
                return JSONResponse(content=response.model_dump())

            processed_first = await self._chain.after_inference(
                body, first_chunk, context
            )
            if isinstance(initial_response, list):
                substitute_first = [processed_first, *initial_response[1:]]
            else:
                substitute_first = processed_first

            wrapped_gen = _stream_with_completion_hook(
                self._chain,
                context,
                gen,
                substitute_first=substitute_first,
            )
            openai_stream = _openai_json_wrapper(wrapped_gen)
            return StreamingResponse(openai_stream, media_type="text/event-stream")
