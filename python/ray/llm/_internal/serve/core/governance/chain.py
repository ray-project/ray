from typing import Any, Dict, List, Union

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


class MiddlewareChain:
    """Run governance middleware hooks in explicit list order."""

    def __init__(self, middlewares: List[LLMMiddleware]) -> None:
        self._middlewares = middlewares

    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        for middleware in self._middlewares:
            try:
                result = await middleware.before_inference(request, context)
            except Exception:
                logger.exception(
                    "Governance middleware %r failed in before_inference",
                    middleware,
                )
                raise
            if isinstance(result, BlockedResponse):
                return result
            request = result
        return request

    async def after_inference(
        self,
        request: Any,
        response: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        for middleware in self._middlewares:
            try:
                result = await middleware.after_inference(request, response, context)
            except Exception:
                logger.exception(
                    "Governance middleware %r failed in after_inference",
                    middleware,
                )
                raise
            if isinstance(result, BlockedResponse):
                return result
            response = result
        return response

    async def on_inference_complete(
        self,
        usage: Dict[str, Any],
        context: RequestContext,
    ) -> None:
        for middleware in self._middlewares:
            try:
                await middleware.on_inference_complete(usage, context)
            except Exception:
                logger.exception(
                    "Governance middleware %r failed in on_inference_complete",
                    middleware,
                )
