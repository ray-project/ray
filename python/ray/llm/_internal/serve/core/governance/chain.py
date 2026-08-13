from typing import Any, Dict, List, Union

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)
from ray.llm._internal.serve.core.governance.middleware import LLMMiddleware


class MiddlewareChain:
    def __init__(self, middlewares: List[LLMMiddleware]) -> None:
        self._middlewares = middlewares

    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        for middleware in self._middlewares:
            result = await middleware.before_inference(request, context)
            if isinstance(result, BlockedResponse):
                return result
            request = result
        return request

    async def after_inference(
        self,
        request: Any,
        response: Any,
        context: RequestContext,
    ) -> Any:
        for middleware in self._middlewares:
            response = await middleware.after_inference(request, response, context)
        return response

    async def on_inference_complete(
        self,
        usage: Dict[str, Any],
        context: RequestContext,
    ) -> None:
        for middleware in self._middlewares:
            await middleware.on_inference_complete(usage, context)
