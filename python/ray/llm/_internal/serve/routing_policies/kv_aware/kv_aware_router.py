import logging
import random
from typing import List, Optional

from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray.llm._internal.serve.routing_policies.kv_aware.constants import (
    DEFAULT_SESSION_AFFINITY_TTL_S,
    REQUEST_TOKEN_IDS_KWARG,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_token_tracker import (
    get_kv_token_tracker,
    get_worker_id,
)
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.request_router.common import PendingRequest
from ray.serve._private.request_router.replica_wrapper import RunningReplica
from ray.serve._private.request_router.request_router import (
    MultiplexMixin,
    RequestRouter,
)
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


class KVAwareRouter(MultiplexMixin, RequestRouter):
    """Routes each request to the candidate that best balances expected KV-cache
    overlap against the worker's current prefill/decode load.

    Scoring is delegated to the ``KVTokenTracker`` (which owns the
    Dynamo selection service and the global KV index) built by the LLMRouter in
    this same ingress process; this per-handle router stays thin and simply maps
    candidate replicas to/from Dynamo worker ids.

    Requests carrying a session id (the ``x-session-id`` header) get session
    affinity: a session's first turn is scored normally and later turns go to
    the replica that served the previous turn, which holds the session's
    prefix in its KV cache.
    """

    def initialize_state(
        self,
        session_affinity_ttl_s: float = DEFAULT_SESSION_AFFINITY_TTL_S,
        session_affinity_max_imbalance: Optional[int] = None,
    ):
        """Bind to the ``KVTokenTracker`` the LLMRouter registered in this
        process. When absent (e.g. the proxy's fallback router), KV-aware
        routing degrades to load-balanced selection instead of erroring.

        Args:
            session_affinity_ttl_s: How long an idle session stays bound to the
                replica that served its last turn. ``0`` disables session
                affinity.
            session_affinity_max_imbalance: If set, a session moves off its
                replica when that replica has more than this many in-flight
                requests above the least-loaded candidate. By default a
                session stays on its replica while the replica is a candidate.
        """
        if session_affinity_ttl_s < 0:
            raise ValueError("session_affinity_ttl_s must be >= 0.")
        if (
            session_affinity_max_imbalance is not None
            and session_affinity_max_imbalance < 0
        ):
            raise ValueError("session_affinity_max_imbalance must be >= 0.")
        self._session_affinity_ttl_s = session_affinity_ttl_s
        self._session_affinity_max_imbalance = session_affinity_max_imbalance
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

        A LoRA request is narrowed to the replicas already holding the adapter
        before it is scored, and carries the adapter name into scoring.

        Args:
            candidate_replicas: The replicas eligible to serve the request.
            pending_request: The request being routed.

        Returns:
            Ranked groups of replicas.
        """
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            # Score within the multiplex candidates
            candidate_replica_ids = self.apply_multiplex_routing(
                pending_request=pending_request
            )
            candidate_replicas = [
                replica
                for replica in candidate_replicas
                if replica.replica_id in candidate_replica_ids
            ]
            if not candidate_replicas:
                # No multiplex candidates yet (e.g. the adapter's location is
                # still propagating). Return no ranks so the caller backs off.
                return []

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
            # vLLM stamps this adapter name on the request's KV-cache events and
            # the selection service salts its KV hashes with it, so each adapter
            # gets its own KV namespace. Empty means the base model.
            lora_name=pending_request.metadata.multiplexed_model_id or None,
            session_id=pending_request.metadata.session_id or None,
            session_affinity_ttl_s=self._session_affinity_ttl_s,
            session_affinity_max_imbalance=self._session_affinity_max_imbalance,
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
