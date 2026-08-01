import logging
import random
from typing import List, Optional

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    get_kv_token_tracker,
    get_worker_id,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import RequestRouter
from ray.serve.config import RequestRouterConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _get_expected_output_tokens(pending_request: PendingRequest) -> Optional[int]:
    """The request's output cap from the routing payload, if present."""
    if not pending_request.args:
        return None
    payload = pending_request.args[0]
    for field in ("max_completion_tokens", "max_tokens"):
        value = getattr(payload, field, None)
        if isinstance(value, int) and value > 0:
            return value
    return None


class KVAwareRouter(RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.

    Scoring is delegated to the ``KVTokenTracker`` (which owns the
    Dynamo selection service and the global KV index) built by the LLMRouter in
    this same ingress process; this per-handle router stays thin and simply maps
    candidate replicas to/from Dynamo worker ids.
    """

    def initialize_state(self):
        """Bind to the ``KVTokenTracker`` the LLMRouter registered in this
        process. When absent (e.g. the proxy's fallback router), KV-aware
        routing degrades to load-balanced selection instead of erroring.
        """
        self._kv_token_tracker = get_kv_token_tracker()
        if self._kv_token_tracker is None:
            logger.warning(
                "No KVTokenTracker in this process (%s); KVAwareRouter "
                "degrades to load-balanced selection here.",
                self._deployment_id,
            )

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        """Choose the candidate replica(s) to route ``pending_request`` to.

        Maps the candidate replicas to their Dynamo worker ids, asks the
        ``KVTokenTracker`` to rank them via ``select_worker``, and
        routes to the chosen worker's replica. With direct streaming enabled,
        HAProxy then forwards the original request to that replica.

        Requests with no prompt token ids have nothing to score on, so they
        route to a random candidate (batch prompts, truncated or unparseable
        bodies).

        Args:
            candidate_replicas: The replicas eligible to serve the request.
            pending_request: The request being routed.

        Returns:
            Ranked groups of replicas.
        """
        token_ids = (
            pending_request.kwargs.get(REQUEST_TOKEN_IDS_KWARG)
            if pending_request is not None
            else None
        )
        # No token ids to score on, or no tracker in this process (proxy fallback
        # router): load-balance.
        if not token_ids or self._kv_token_tracker is None:
            return [[random.choice(candidate_replicas)]] if candidate_replicas else []

        worker_id_to_replica = {
            get_worker_id(replica.replica_id.unique_id): replica
            for replica in candidate_replicas
        }
        selection = await self._kv_token_tracker.select_worker(
            pending_request.metadata.request_id,
            token_ids,
            list(worker_id_to_replica),
            _get_expected_output_tokens(pending_request),
        )
        return [[worker_id_to_replica[selection["worker_id"]]]]


def is_kv_aware(llm_config: LLMConfig) -> bool:
    """Whether ``llm_config`` selects a ``KVAwareRouter`` for replica selection."""
    request_router_config = llm_config.deployment_config.get("request_router_config")
    if isinstance(request_router_config, dict):
        request_router_config = RequestRouterConfig(**request_router_config)
    return isinstance(request_router_config, RequestRouterConfig) and issubclass(
        request_router_config.get_request_router_class(), KVAwareRouter
    )
