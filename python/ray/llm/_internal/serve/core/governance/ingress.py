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
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


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


async def _aclose_quietly(*gens: Any) -> None:
    """Close async generators without raising.

    ``_peek_at_generator`` returns a replay wrapper. Closing that wrapper
    does not close the upstream LLM generator if the wrapper has not been
    iterated, so callers pass both.
    """
    for gen in gens:
        aclose = getattr(gen, "aclose", None)
        if aclose is None:
            continue
        try:
            await aclose()
        except Exception:
            logger.exception("Failed to close governance stream generator")


async def _stream_with_completion_hook(
    chain: MiddlewareChain,
    context: RequestContext,
    gen: AsyncGenerator,
    *,
    substitute_first: Any = None,
    extra_close: tuple = (),
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
        await _aclose_quietly(gen, *extra_close)
        try:
            await chain.on_inference_complete(last_usage, context)
        except Exception:
            logger.exception(
                "Failed to run governance on_inference_complete hooks model=%s request_id=%s",
                context.model_id,
                context.request_id,
            )


async def _sse_stream_with_completion_hook(
    chain: MiddlewareChain,
    context: RequestContext,
    gen: AsyncGenerator,
    *,
    substitute_first: Any = None,
    extra_close: tuple = (),
) -> AsyncGenerator[str, None]:
    """Wrap chunks as SSE and aclose the inner generator on client disconnect.

    ``_openai_json_wrapper`` does not aclose its source, so without this wrapper
    ``on_inference_complete`` would only run when the inner generator is
    garbage-collected.
    """
    hook_gen = _stream_with_completion_hook(
        chain,
        context,
        gen,
        substitute_first=substitute_first,
        extra_close=extra_close,
    )
    try:
        async for packet in _openai_json_wrapper(hook_gen):
            yield packet
    finally:
        await _aclose_quietly(hook_gen)


class GovernanceIngress(OpenAiIngress):
    """OpenAI ingress with governance middleware hooks on the inference path."""

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
            logger.info(
                "Governance blocked request before inference model=%s request_id=%s rule=%s",
                context.model_id,
                context.request_id,
                before_result.rule_triggered,
            )
            return blocked_response_to_http(before_result)
        body = before_result

        async with router_request_timeout(DEFAULT_LLM_ROUTER_HTTP_TIMEOUT):
            upstream = self._get_response(
                body=body, call_method=call_method, raw_request=raw_request
            )
            initial_response, gen = await _peek_at_generator(upstream)

            if isinstance(initial_response, list):
                first_chunk = initial_response[0]
            else:
                first_chunk = initial_response

            if isinstance(first_chunk, ErrorResponse):
                await _aclose_quietly(gen, upstream)
                raise OpenAIHTTPException(
                    message=first_chunk.error.message,
                    status_code=first_chunk.error.code,
                    type=first_chunk.error.type,
                )

            if isinstance(first_chunk, NON_STREAMING_RESPONSE_TYPES):
                try:
                    response = await self._chain.after_inference(
                        body, first_chunk, context
                    )
                    if isinstance(response, BlockedResponse):
                        logger.info(
                            "Governance blocked response after inference model=%s request_id=%s rule=%s",
                            context.model_id,
                            context.request_id,
                            response.rule_triggered,
                        )
                        return blocked_response_to_http(response)
                    await self._chain.on_inference_complete(
                        usage_to_dict(response), context
                    )
                    return JSONResponse(content=response.model_dump())
                finally:
                    await _aclose_quietly(gen, upstream)

            try:
                processed_first = await self._chain.after_inference(
                    body, first_chunk, context
                )
            except Exception:
                await _aclose_quietly(gen, upstream)
                raise
            if isinstance(processed_first, BlockedResponse):
                logger.info(
                    "Governance blocked streaming response model=%s request_id=%s rule=%s",
                    context.model_id,
                    context.request_id,
                    processed_first.rule_triggered,
                )
                await _aclose_quietly(gen, upstream)
                return blocked_response_to_http(processed_first)
            if isinstance(initial_response, list):
                substitute_first = [processed_first, *initial_response[1:]]
            else:
                substitute_first = processed_first

            openai_stream = _sse_stream_with_completion_hook(
                self._chain,
                context,
                gen,
                substitute_first=substitute_first,
                extra_close=(upstream,),
            )
            return StreamingResponse(openai_stream, media_type="text/event-stream")
