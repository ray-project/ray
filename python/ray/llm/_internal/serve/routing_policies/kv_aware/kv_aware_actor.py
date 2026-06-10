import asyncio
import hashlib
import logging
import math
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

from vllm.config import CacheConfig

import ray
from ray import serve
from ray.serve._private.common import DeploymentTargetInfo, ReplicaID
from ray.serve._private.constants import (
    SERVE_CONTROLLER_NAME,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.long_poll import LongPollClient, LongPollNamespace

logger = logging.getLogger(SERVE_LOGGER_NAME)

KV_ROUTER_ACTOR_NAME = "serve_llm_kv_router"

#: Hooks a replica may invoke through ``KVRouterActor.on_lifecycle_events``.
LIFECYCLE_HOOKS = frozenset(
    {
        "on_request_added",
        "on_prefill_complete",
        "on_decode_progress",
        "on_request_completed",
    }
)


def get_worker_id(replica_unique_id: str) -> int:
    """
    Deterministically derive a Dynamo worker id from a replica's unique id.
    """
    return int.from_bytes(
        hashlib.blake2b(replica_unique_id.encode(), digest_size=8).digest(), "big"
    )


def derive_kv_block_size(engine_kwargs: Dict[str, Any]) -> int:
    return CacheConfig(block_size=engine_kwargs.get("block_size")).block_size


@dataclass
class RequestLifecycle:
    """In-flight request load state while the request is served by a replica."""

    worker_id: int
    prompt_tokens: int = 0
    prefill_completed: bool = False
    output_tokens: int = 0
    output_blocks: int = 0
    total_blocks: int = 0


@ray.remote
class KVRouterActor:
    """Deployment-scoped Ray actor hosting the KV-aware router.

    KVRouterActor, independent of any replica's lifetime, is attached to the LLMServer
    deployment via Serve's DeploymentActorConfig. It exposes the KV-aware routing interfaces:
    - Replica membership tracking
    - KV-aware scoring

    TODO (jeffreywang): The radix tree that backs them lands in a later PR.
    """

    def __init__(self, block_size: int):
        """Initialize the actor.

        Args:
            block_size: The deployment's KV-cache block size in tokens required for
                decode-block accounting.
        """
        self._block_size = block_size
        self._replica_id_by_worker: Dict[int, str] = {}
        self._requests: Dict[str, RequestLifecycle] = {}
        self._long_poll_client: Optional[LongPollClient] = None
        self._start_replica_tracking()

    def get_block_size(self) -> int:
        """Return the KV-cache block size this actor uses for decode-block accounting."""
        return self._block_size

    def _start_replica_tracking(self) -> None:
        """Subscribe to this deployment's running replicas via LongPollClient."""
        deployment_id = serve.get_deployment_actor_context().deployment_id
        controller = ray.get_actor(SERVE_CONTROLLER_NAME, namespace=SERVE_NAMESPACE)
        self._long_poll_client = LongPollClient(
            controller,
            {
                (
                    LongPollNamespace.DEPLOYMENT_TARGETS,
                    deployment_id,
                ): self._on_deployment_targets,
            },
            call_in_event_loop=asyncio.get_event_loop(),
            client_id=f"{type(self).__name__}:{deployment_id}",
        )

    def _on_deployment_targets(self, target_info: DeploymentTargetInfo) -> None:
        """LongPoll listener: sync the worker mapping to the running replicas.

        Diffs the snapshot against the current mapping and updates the reverse
        worker->replica map.
        """
        new = {
            get_worker_id(r.replica_id.unique_id): r.replica_id.to_full_id_str()
            for r in target_info.running_replicas
        }
        current = set(self._replica_id_by_worker)
        added = new.keys() - current
        removed = current - new.keys()

        for worker_id in removed:
            self.remove_worker(worker_id)
            self._replica_id_by_worker.pop(worker_id, None)
        for worker_id in added:
            self._replica_id_by_worker[worker_id] = new[worker_id]
            self.add_worker(worker_id)

        if added or removed:
            logger.info(
                "KV router replica membership updated: +%d -%d, tracking %d worker(s).",
                len(added),
                len(removed),
                len(self._replica_id_by_worker),
            )

    def get_tracked_worker_id(self, replica_id_full_str: str) -> Optional[int]:
        """Return the worker id for a currently-tracked replica, or ``None``."""
        unique_id = ReplicaID.from_full_id_str(replica_id_full_str).unique_id
        worker_id = get_worker_id(unique_id)
        return worker_id if worker_id in self._replica_id_by_worker else None

    def get_replica_id(self, worker_id: int) -> Optional[str]:
        """Return the replica's full id string for ``worker_id``, or ``None``."""
        return self._replica_id_by_worker.get(worker_id)

    def get_candidate_worker_ids(self) -> List[int]:
        """Return the currently tracked worker ids, sorted ascending."""
        return sorted(self._replica_id_by_worker)

    def add_worker(self, worker_id: int) -> None:
        """Register a worker with the KV router when a replica is added."""
        pass

    def remove_worker(self, worker_id: int) -> None:
        """Deregister a worker from the KV router when a replica is removed."""
        pass

    async def on_lifecycle_events(self, events: List[tuple]) -> None:
        """Apply a replica's ``(hook_name, args)`` lifecycle events in order.

        Replicas deliver events through their reporter's ordered pump (one
        awaited batch in flight per replica) because plain fire-and-forget
        calls to an async actor are not executed in submission order, and the
        lifecycle hooks are order-sensitive (e.g. a completion overtaking the
        admission would resurrect an evicted request).
        """
        for hook_name, args in events:
            if hook_name in LIFECYCLE_HOOKS:
                await getattr(self, hook_name)(*args)
            else:
                logger.debug("Ignoring unknown lifecycle hook %s", hook_name)

    async def on_request_added(
        self,
        request_id: str,
        worker_id: int,
        prompt_token_count: int = 0,
    ) -> None:
        """Record a newly admitted request as active load on ``worker_id``."""
        state = RequestLifecycle(
            worker_id=worker_id,
            prompt_tokens=prompt_token_count,
            total_blocks=math.ceil(prompt_token_count / self._block_size),
        )
        self._requests[request_id] = state

    async def on_prefill_complete(self, request_id: str) -> None:
        """Mark that ``request_id`` completed prefill and is now decoding."""
        state = self._requests.get(request_id)
        if state is not None:
            state.prefill_completed = True

    async def on_decode_progress(
        self, request_id: str, cumulative_output_tokens: int
    ) -> None:
        """Advance ``request_id`` to an exact cumulative output-token count."""
        state = self._requests.get(request_id)
        if state is None:
            return
        state.output_tokens = cumulative_output_tokens
        new_total_blocks = math.ceil(
            (state.prompt_tokens + cumulative_output_tokens) / self._block_size
        )
        while new_total_blocks > state.total_blocks:
            state.output_blocks += 1
            state.total_blocks += 1

    async def on_request_completed(self, request_id: str) -> None:
        """Evict ``request_id``; it no longer counts as active load."""
        self._requests.pop(request_id, None)

    async def get_request_lifecycle(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Return a snapshot of an in-flight request's state, or ``None``."""
        state = self._requests.get(request_id)
        return None if state is None else asdict(state)

    async def get_active_request_ids(self) -> List[str]:
        """Return ids of the in-flight requests."""
        return list(self._requests)

    async def get_worker_active_load(self, worker_id: int) -> int:
        """Return the number of in-flight requests attributed to ``worker_id``."""
        return sum(1 for s in self._requests.values() if s.worker_id == worker_id)

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

    async def add_output_block(
        self,
        request_id: str,
        decay_fraction: Optional[float] = None,
    ) -> None:
        """Account one newly generated KV block for a decoding request.

        Args:
            request_id: Unique identifier for the request.
            decay_fraction: Estimated fraction of output still to come, in
                ``[0, 1]``; ``None`` when no output-length estimate exists.
        """
        raise NotImplementedError("KVRouterActor.add_output_block is not implemented")

    async def free(self, request_id: str) -> None:
        """Release the KV-cache accounting for a finished request.

        Args:
            request_id: Unique identifier for the request.
        """
        raise NotImplementedError("KVRouterActor.free is not implemented")
