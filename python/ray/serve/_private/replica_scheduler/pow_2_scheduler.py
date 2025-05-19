import logging
import random
from typing import (
    List,
    Optional,
)

from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
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


class PowerOfTwoChoicesReplicaScheduler(
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

    async def choose_replicas(
        self,
        replicas_ranks: List[List[RunningReplica]],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
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

        chosen_ids = random.sample(
            list(candidate_replica_ids),
            k=min(2, len(candidate_replica_ids)),
        )
        replica_id_to_replica_map = {
            replica.replica_id: replica for replica in replicas_ranks[0]
        }
        return [[replica_id_to_replica_map[chosen_id] for chosen_id in chosen_ids]]
