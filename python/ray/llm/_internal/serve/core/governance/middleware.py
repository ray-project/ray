from abc import ABC, abstractmethod
from typing import Any, Dict, Union

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)


class LLMMiddleware(ABC):
    @abstractmethod
    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        """Run pre-inference checks. Return request to proceed or BlockedResponse to block."""
        ...

    @abstractmethod
    async def after_inference(
        self,
        request: Any,
        response: Any,
        context: RequestContext,
    ) -> Any:
        """Run post-inference checks. Return response (potentially modified)."""
        ...

    async def on_inference_complete(
        self,
        usage: Dict[str, Any],
        context: RequestContext,
    ) -> None:
        """Called after inference completes. Override to reconcile token usage."""
        pass
