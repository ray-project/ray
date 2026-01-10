"""Collective RPC ingress mixin.

Provides HTTP endpoint for collective RPC operations across all replicas
and their workers, enabling RLHF workflows where a trainer forms a single
NCCL process group with all TP/PP workers across all replicas.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from ray.llm._internal.serve.core.ingress.mixins.broadcastable import (
    ReplicaBroadcastable,
)
from ray.llm._internal.serve.observability.logging import get_logger

logger = get_logger(__name__)


# --- Pydantic Models ---


class CollectiveRpcRequest(BaseModel):
    """Request to execute a collective RPC on all replicas."""

    model: str
    method: str
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    timeout: Optional[float] = None


class ReplicaResult(BaseModel):
    """Result from a single replica containing all worker results."""

    replica: int
    worker_results: List[Any]


class CollectiveRpcResponse(BaseModel):
    """Response containing results from all replicas."""

    results: List[ReplicaResult]


# --- Mixin ---


class CollectiveRpcIngressMixin(ReplicaBroadcastable):
    """Ingress mixin for /collective_rpc endpoint.

    Adds control plane endpoint for executing collective RPC calls across
    all replicas and their workers. This is used for RLHF workflows where
    a trainer needs to communicate with all TP/PP workers across all replicas.
    """

    ENDPOINTS = {
        "collective_rpc": lambda app: app.post("/collective_rpc"),
    }

    async def collective_rpc(self, body: CollectiveRpcRequest) -> CollectiveRpcResponse:
        """Execute a collective RPC on all replicas for the specified model.

        This broadcasts the RPC call to all replicas, and each replica
        executes the call on all its workers (TP/PP ranks).

        Args:
            body: Request containing the model ID, method name, args, kwargs,
                and optional timeout.

        Returns:
            CollectiveRpcResponse with results from all replicas.
        """
        logger.info(
            "Executing collective_rpc '%s' for model %s with args=%s, kwargs=%s",
            body.method,
            body.model,
            body.args,
            body.kwargs,
        )

        # Broadcast to all replicas - each replica returns a list of worker results
        replica_results = await self._broadcast_to_replicas(
            body.model,
            "collective_rpc",
            kwargs={
                "method": body.method,
                "args": tuple(body.args),
                "kwargs": body.kwargs,
                "timeout": body.timeout,
            },
        )

        # Format results with replica index for debugging
        results = [
            ReplicaResult(replica=i, worker_results=worker_results or [])
            for i, worker_results in enumerate(replica_results or [])
        ]

        return CollectiveRpcResponse(results=results)
