"""Sleepable ingress mixin.

Provides HTTP endpoints for sleep/wakeup control plane operations.
"""

from typing import Any, Dict

from fastapi import Query
from pydantic import BaseModel, Field
from starlette.responses import Response

from ray.llm._internal.serve.core.ingress.mixins.broadcastable import (
    ReplicaBroadcastable,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


# --- Pydantic Models ---


class SleepRequest(BaseModel):
    """Request to put an engine to sleep."""

    model: str
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-specific sleep options (e.g., level for vLLM)",
    )


class WakeupRequest(BaseModel):
    """Request to wake up an engine from sleep."""

    model: str
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-specific wakeup options (e.g., tags for vLLM)",
    )


class IsSleepingResponse(BaseModel):
    """Response indicating whether the engine is sleeping."""

    is_sleeping: bool


# --- Mixin ---


class SleepableIngressMixin(ReplicaBroadcastable):
    """Ingress mixin for /sleep, /wakeup, /is_sleeping endpoints.

    Adds control plane endpoints for managing engine sleep state.
    Sleep mode offloads model weights to CPU and discards KV cache.
    """

    ENDPOINTS = {
        "sleep": lambda app: app.post("/sleep"),
        "wakeup": lambda app: app.post("/wakeup"),
        "is_sleeping": lambda app: app.get("/is_sleeping"),
    }

    async def sleep(self, body: SleepRequest) -> Response:
        """Put the engine to sleep on all replicas for the specified model.

        This offloads model weights to CPU and discards KV cache, freeing
        GPU memory. The engine cannot process requests while sleeping.

        Args:
            body: Request containing the model ID and engine-specific options.

        Returns:
            200 OK on success.
        """
        logger.info(
            "Putting model %s to sleep with options: %s", body.model, body.options
        )
        await self._broadcast_to_replicas(body.model, "sleep", kwargs=body.options)
        return Response(status_code=200)

    async def wakeup(self, body: WakeupRequest) -> Response:
        """Wake up the engine from sleep on all replicas for the specified model.

        Args:
            body: Request containing the model ID and engine-specific options.

        Returns:
            200 OK on success.
        """
        logger.info("Waking up model %s with options: %s", body.model, body.options)
        await self._broadcast_to_replicas(body.model, "wakeup", kwargs=body.options)
        return Response(status_code=200)

    async def is_sleeping(
        self, model: str = Query(..., description="The model ID to check")
    ) -> IsSleepingResponse:
        """Check if the engine is sleeping for the specified model.

        This checks the sleep status across all replicas. Returns True if
        ANY replica is sleeping (uses logical OR across replicas).

        Args:
            model: The model ID to check.

        Returns:
            IsSleepingResponse with is_sleeping boolean.
        """
        results = await self._broadcast_to_replicas(model, "is_sleeping")
        is_sleeping_result = any(results) if results else False
        return IsSleepingResponse(is_sleeping=is_sleeping_result)
