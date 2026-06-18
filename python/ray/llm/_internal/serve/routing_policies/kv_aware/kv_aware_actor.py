import logging
from typing import List, Optional, TypedDict

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"


class WorkerSelection(TypedDict):
    """The worker chosen by ``KVRouterActor.select_worker`` for a request."""

    # The chosen worker.
    worker_id: int
    # Data-parallel rank within the worker.
    dp_rank: int
    # KV blocks already cached on that worker.
    overlap_blocks: int
    # The worker's routing score.
    score: float


@ray.remote
class KVRouterActor:
    """Deployment-scoped Ray actor backing KV-aware routing.

    Attached to the LLMServer deployment via Serve's ``DeploymentActorConfig``,
    independent of any replica's lifetime. It exposes the KV-aware routing
    interfaces: ``select_worker`` and the request-lifecycle hooks.

    TODO (jeffreywang): Implement these
    1. Created once per deployment, attached to the LLMServer deployment via
       Serve's ``DeploymentActorConfig`` (independent of any replica's lifetime).
    2. Owns an in-process Dynamo ``SelectionService``.
    3. Tracks live replicas via a ``LongPollClient`` on ``DEPLOYMENT_TARGETS``:
       replicas advertise their engine KV-events endpoint through
       ``record_routing_stats``, and ``_on_deployment_targets`` registers new
       workers (the service dials them connect-out) and evicts departed ones.
    4. The ``SelectionService`` maintains a global KV index radix tree, fed by
       every replica's KV events; each node records which workers hold that KV
       block.
    5. Scoring ranks candidate workers by KV-cache overlap (queried from the KV
       index) plus prefill/decode load to pick the best worker.
    """

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
    ) -> WorkerSelection:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            The selected worker (see ``WorkerSelection``).
        """
        raise NotImplementedError("KVRouterActor.select_worker is not implemented")

    async def on_request_added(
        self,
        request_id: str,
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Commit a routed request to the worker chosen by ``select_worker``.

        Args:
            request_id: Unique identifier for the request.
            expected_output_tokens: Predicted number of output tokens.
        """
        raise NotImplementedError("KVRouterActor.on_request_added is not implemented")

    async def on_prefill_complete(self, request_id: str) -> None:
        """Record a request's transition from prefill to decode.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_prefill_complete is not implemented"
        )

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Adds any newly crossed decode blocks to the request's accounted load.

        Args:
            request_id: Unique identifier for the request.
            cumulative_output_tokens: Total output tokens generated so far.
        """
        raise NotImplementedError("KVRouterActor.on_decode_progress is not implemented")

    async def on_request_completed(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.on_request_completed is not implemented"
        )
