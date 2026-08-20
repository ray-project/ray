"""Governance middleware hook interface for Ray Serve LLM."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Union

from ray.llm._internal.serve.core.governance.context import (
    BlockedResponse,
    RequestContext,
)


class LLMMiddleware(ABC):
    """Hook interface for request/response governance.

    ``before_inference`` is required. ``after_inference`` and
    ``on_inference_complete`` have pass-through defaults so a middleware
    can implement only the hooks it needs.
    """

    @abstractmethod
    async def before_inference(
        self,
        request: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        """Run pre-inference checks. Return request to proceed or BlockedResponse to block."""
        ...

    async def after_inference(
        self,
        request: Any,
        response: Any,
        context: RequestContext,
    ) -> Union[Any, BlockedResponse]:
        """Run post-inference checks.

        Return the response (potentially modified) or a BlockedResponse.
        The default passes the response through unchanged.
        """
        return response

    async def on_inference_complete(
        self,
        usage: Dict[str, Any],
        context: RequestContext,
    ) -> None:
        """Called after inference completes. Override to reconcile token usage."""
        pass
