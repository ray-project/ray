import logging
import random
from typing import (
    List,
    Optional,
)

from ray.serve._private.constants import (
    SERVE_LOGGER_NAME,
)
from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    FIFOMixin,
    LocalityMixin,
    MultiplexMixin,
    RequestRouter,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PowerOfTwoChoicesRequestRouter(
    FIFOMixin, LocalityMixin, MultiplexMixin, RequestRouter
):
    """Chooses a replica for each request using the "power of two choices" procedure.

    Requests are routed in FIFO order.

    When a request comes in, two candidate replicas are chosen randomly. Each replica
    is sent a control message to fetch its queue length.

    The replica responds with two items: (queue_len, accepted). Only replicas that
    accept the request are considered; between those, the one with the lower queue
    length is chosen.

    In the case when neither replica accepts the request (e.g., their queues are full),
    the procedure is repeated with backoff. This backoff repeats indefinitely until a
    replica is chosen, so the caller should use timeouts and cancellation to avoid
    hangs.

    Each request being routed may spawn an independent task that runs the routing
    procedure concurrently. This task will not necessarily satisfy the request that
    started it (in order to maintain the FIFO order). The total number of tasks is
    capped at (2 * num_replicas).
    """

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
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
            candidate_replica_ids = self.apply_multiplex_routing(
                pending_request=pending_request,
            )
        else:
            # Get candidates for locality preference.
            candidate_replica_ids = self.apply_locality_routing(
                pending_request=pending_request,
            )

        if not candidate_replica_ids:
            return []

        # Optimized selection: use direct randrange for k=2 instead of random.sample.
        # This is ~1.9x faster for the common case of selecting 2 replicas.
        #
        # Correctness proof: We pick i uniformly from [0, n), then j uniformly from
        # [0, n-1) and shift j up if j >= i. Every ordered pair (i, j) with i != j
        # has probability: Pr(i,j) = 1/n * 1/(n-1) = 1/(n(n-1))
        # This matches random.sample(k=2): uniform among all 2-permutations.
        candidates = tuple(candidate_replica_ids)
        n = len(candidates)
        if n == 1:
            chosen_ids = [candidates[0]]
        elif n == 2:
            # Randomize order to ensure fair selection when queue lengths are equal
            if random.getrandbits(1):
                chosen_ids = [candidates[0], candidates[1]]
            else:
                chosen_ids = [candidates[1], candidates[0]]
        else:
            i = random.randrange(n)
            j = random.randrange(n - 1)
            if j >= i:
                j += 1
            chosen_ids = [candidates[i], candidates[j]]

        return [[self._replicas[chosen_id] for chosen_id in chosen_ids]]
