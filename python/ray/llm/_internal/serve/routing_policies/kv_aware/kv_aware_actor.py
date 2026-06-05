import logging
from typing import Any, Dict, List, Optional

import ray
from ray.serve._private.constants import SERVE_LOGGER_NAME

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"


@ray.remote
class KVRouterActor:
    # TODO (jeffreywang): In subsequent PRs, KVRouterActor will host the global KV
    # radix tree for KV-aware request scoring.

    async def select_worker(
        self,
        request_id: str,
        token_ids: List[int],
        allowed_worker_ids: List[int],
    ) -> Dict[str, Any]:
        """Score the allowed workers for a request based on KV-cache overlap and
        load and pick the best one.

        Args:
            request_id: Unique identifier for the request being routed.
            token_ids: Prompt token ids used to compute KV-cache overlap.
            allowed_worker_ids: Candidate worker ids the router may select from.

        Returns:
            A dict describing the selected worker:
            ``worker_id`` (int): the chosen worker.
            ``dp_rank`` (int): data-parallel rank within the worker.
            ``overlap_blocks`` (int): KV blocks already cached on that worker.
            ``score`` (float): the worker's routing score (higher is better).
        """
        raise NotImplementedError("KVRouterActor.select_worker is not implemented")

    async def add_request(
        self,
        request_id: str,
        expected_output_tokens: Optional[int] = None,
    ) -> None:
        """Commit a routed request to the worker chosen by ``select_worker``.

        Args:
            request_id: Unique identifier for the request.
            expected_output_tokens: Predicted number of output tokens, used to
                estimate the request's decode-phase KV footprint.
        """
        raise NotImplementedError("KVRouterActor.add_request is not implemented")

    async def mark_prefill_complete(self, request_id: str) -> None:
        """Signals the transition from prefill to decode.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError(
            "KVRouterActor.mark_prefill_complete is not implemented"
        )

    async def free(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError("KVRouterActor.free is not implemented")
