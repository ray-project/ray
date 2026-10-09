"""Cache manager ingress mixin.

Provides HTTP endpoints for cache management control plane operations.
"""

from pydantic import BaseModel
from starlette.responses import Response

from ray.llm._internal.serve.core.ingress.mixins.broadcastable import (
    ReplicaBroadcastable,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


# --- Pydantic Models ---


class ResetPrefixCacheRequest(BaseModel):
    """Request to reset the prefix cache."""

    model: str


# --- Mixin ---


class CacheManagerIngressMixin(ReplicaBroadcastable):
    """Ingress mixin for /reset_prefix_cache endpoint.

    Adds control plane endpoint for managing the KV prefix cache.
    """

    ENDPOINTS = {
        "reset_prefix_cache": lambda app: app.post("/reset_prefix_cache"),
    }

    async def reset_prefix_cache(self, body: ResetPrefixCacheRequest) -> Response:
        """Reset the KV prefix cache on all replicas for the specified model.

        Clears cached key-value pairs from previous requests. Useful for
        benchmarking or when cache invalidation is needed.

        Args:
            body: Request containing the model ID.

        Returns:
            200 OK on success.
        """
        logger.info("Resetting prefix cache for model: %s", body.model)
        await self._broadcast_to_replicas(body.model, "reset_prefix_cache")
        return Response(status_code=200)
