import logging
import random
import time
from typing import (
    List,
    Optional,
    Set,
)

from ray.llm._internal.serve.replica_scheduler.prefix_aware.prefix_tree import (
    PrefixTreeActor,
)
from ray.serve._private.common import ReplicaID
from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
from ray.serve._private.replica_result import ReplicaResult
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
)
from ray.serve._private.replica_scheduler.replica_scheduler import (
    FIFOMixin,
    LocalityScheduleMixin,
    MultiplexScheduleMixin,
    ReplicaScheduler,
)
from ray.serve._private.replica_scheduler.replica_wrapper import (
    RunningReplica,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PrefixAwareReplicaSchedulerSimple(
    FIFOMixin, MultiplexScheduleMixin, LocalityScheduleMixin, ReplicaScheduler
):
    """Chooses a replica for each request using the "power of two choices" procedure.

    Requests are scheduled in FIFO order.

    When a request comes in, two candidate replicas are chosen randomly. Each replica
    is sent a control message to fetch its queue length.

    The replica responds with two items: (queue_len, accepted). Only replicas that
    accept the request are considered; between those, the one with the lower queue
    length is chosen.

    In the case when neither replica accepts the request (e.g., their queues are full),
    the procedure is repeated with backoff. This backoff repeats indefinitely until a
    replica is chosen, so the caller should use timeouts and cancellation to avoid
    hangs.

    Each request being scheduled may spawn an independent task that runs the scheduling
    procedure concurrently. This task will not necessarily satisfy the request that
    started it (in order to maintain the FIFO order). The total number of tasks is
    capped at (2 * num_replicas).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._tree_actor = PrefixTreeActor.options(
            name="PrefixTreeActor", get_if_exists=True
        ).remote()
        self.IMBALANCED_THRESHOLD = 10

    async def choose_replicas(
        self,
        replicas_ranks: List[Set[RunningReplica]],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[Set[RunningReplica]]:
        """One iteration of the power of two choices procedure that chooses
         (at most) two random available replicas.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.
        """
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            # Get candidates for multiplexed model ID.
            candidate_replica_ids = self.apply_multiplex_scheduling(
                pending_request=pending_request,
            )
        else:
            # Get candidates for locality preference.
            candidate_replica_ids = self.apply_locality_scheduling(
                pending_request=pending_request,
            )

        if not candidate_replica_ids:
            return []
        chosen_ids = await self.prefix_match_best_replicas(
            pending_request, candidate_replica_ids
        )
        return [{self._replicas[chosen_id] for chosen_id in chosen_ids}]

    def get_input_text(self, pending_request: PendingRequest) -> str:
        chat_completion_request = pending_request.args[0]
        if hasattr(chat_completion_request, "messages"):
            messages = chat_completion_request.messages
            return "".join(
                msg.get("content", "") for msg in messages if "content" in msg
            )
        elif hasattr(chat_completion_request, "prompt"):
            return chat_completion_request.prompt
        else:
            raise ValueError("Invalid chat completion request")

    async def prefix_match_best_replicas(
        self,
        pending_request: Optional[PendingRequest],
        candidate_replica_ids: Set[ReplicaID],
    ) -> List[ReplicaID]:
        """
        Returns a set of candidate replicas, of which the one with the smallest replica queue will be chosen.
        0. Default: same as pow 2 scheduler, return 2 replicas at random.
        1. If load is balanced, choose replica(s) with highest prefix match rate. If highest hit rate is below 10% or no match found, use default.
        2. If load is imbalanced, use default.
        """
        # Convert candidate replica IDs to strings for prefix matching.
        candidate_replica_ids_strings = [
            replica_id.to_full_id_str() for replica_id in candidate_replica_ids
        ]

        # Ensure each candidate replica is an active tenant in the prefix tree.
        for replica_id_string in candidate_replica_ids_strings:
            await self._tree_actor._add_tenant.remote(replica_id_string)

        # Default: return 2 replicas at random.
        chosen_replica_ids_strings = random.sample(
            list(candidate_replica_ids_strings),
            min(2, len(candidate_replica_ids_strings)),
        )

        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = self.get_input_text(pending_request)
            if input_text is not None:
                # Check for imbalanced load.
                highest_queue_len = 0
                lowest_queue_len = float("inf")
                if self._use_replica_queue_len_cache:
                    # Populate available queue lens from the cache.
                    r: ReplicaID
                    for r in candidate_replica_ids:
                        queue_len = self._replica_queue_len_cache.get(r)
                        if queue_len is None:
                            continue
                        else:
                            highest_queue_len = max(highest_queue_len, queue_len)
                            lowest_queue_len = min(lowest_queue_len, queue_len)
                is_imbalanced = (
                    highest_queue_len - lowest_queue_len > self.IMBALANCED_THRESHOLD
                )
                if is_imbalanced:
                    pass
                else:
                    (
                        matched_text,
                        matched_tenant_ids,
                    ) = await self._tree_actor.prefix_match.remote(
                        input_text, candidate_replica_ids_strings
                    )
                    match_rate = len(matched_text) / len(input_text)
                    if match_rate < 0.1:
                        pass
                        smallest_tenants = (
                            await self._tree_actor.get_smallest_tenants.remote()
                        )
                        if smallest_tenants is not None and len(smallest_tenants) > 0:
                            chosen_replica_ids_strings = smallest_tenants
                    else:
                        if (
                            matched_tenant_ids is not None
                            and len(matched_tenant_ids) > 0
                        ):
                            chosen_replica_ids_strings = matched_tenant_ids
        chosen_replica_ids = [
            ReplicaID.from_full_id_str(replica_id_string)
            for replica_id_string in chosen_replica_ids_strings
        ]
        return chosen_replica_ids

    async def on_request_scheduled(
        self,
        pending_request: PendingRequest,
        replica_id: ReplicaID,
        result: ReplicaResult,
    ):
        """Called when a request is scheduled to a replica.

        This is used as a callback to update the state of the scheduler after
        a response is generated.
        """
        # Right now this only inserts the prompt into the prefix tree, not the response (streaming response makes things complicated)
        if (
            pending_request is not None
            and pending_request.args is not None
            and len(pending_request.args) > 0
        ):
            input_text = self.get_input_text(pending_request)
            if input_text is not None:
                await self._tree_actor.insert.remote(
                    input_text, replica_id.to_full_id_str(), time.time()
                )
