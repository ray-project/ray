"""Pausable ingress mixin.

Provides HTTP endpoints for pause/resume control plane operations.
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


class PauseRequest(BaseModel):
    """Request to pause generation on an engine."""

    model: str
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-specific pause options (e.g., wait_for_inflight_requests, clear_cache)",
    )


class ResumeRequest(BaseModel):
    """Request to resume generation on an engine."""

    model: str
    options: Dict[str, Any] = Field(
        default_factory=dict,
        description="Engine-specific resume options",
    )


class IsPausedResponse(BaseModel):
    """Response indicating whether the engine is paused."""

    is_paused: bool


# --- Mixin ---


class PausableIngressMixin(ReplicaBroadcastable):
    """Ingress mixin for /pause, /resume, /is_paused endpoints.

    Adds control plane endpoints for managing engine pause state.
    Pause mode halts generation/encoding while keeping weights in GPU memory.
    Unlike sleep mode, pause does not offload weights to CPU.
    """

    ENDPOINTS = {
        "pause": lambda app: app.post("/pause"),
        "resume": lambda app: app.post("/resume"),
        "is_paused": lambda app: app.get("/is_paused"),
    }

    async def pause(self, body: PauseRequest) -> Response:
        """Pause generation on all replicas for the specified model.

        This halts generation/encoding requests while keeping model weights
        in GPU memory. New requests are blocked until resume is called.
        Unlike sleep mode, pause does not offload weights to CPU.

        Args:
            body: Request containing the model ID and engine-specific options.
                Options may include:
                - wait_for_inflight_requests (bool): Wait for in-flight requests
                  to finish before pausing. Default False (abort immediately).
                - clear_cache (bool): Clear KV cache after draining. Default True.

        Returns:
            200 OK on success.
        """
        logger.info("Pausing model %s with options: %s", body.model, body.options)
        await self._broadcast_to_replicas(body.model, "pause", kwargs=body.options)
        return Response(status_code=200)

    async def resume(self, body: ResumeRequest) -> Response:
        """Resume generation on all replicas for the specified model.

        Args:
            body: Request containing the model ID and engine-specific options.

        Returns:
            200 OK on success.
        """
        logger.info("Resuming model %s with options: %s", body.model, body.options)
        await self._broadcast_to_replicas(body.model, "resume", kwargs=body.options)
        return Response(status_code=200)

    async def is_paused(
        self, model: str = Query(..., description="The model ID to check")
    ) -> IsPausedResponse:
        """Check if the engine is paused for the specified model.

        This checks the pause status across all replicas. Returns True if
        ANY replica is paused (uses logical OR across replicas).

        Args:
            model: The model ID to check.

        Returns:
            IsPausedResponse with is_paused boolean.
        """
        results = await self._broadcast_to_replicas(model, "is_paused")
        is_paused_result = any(results) if results else False
        return IsPausedResponse(is_paused=is_paused_result)
