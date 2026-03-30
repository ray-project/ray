"""Round-robin request router for Ray Serve.

Extends the same mixin chain as the default PowerOfTwoChoicesRequestRouter
(FIFOMixin, LocalityMixin, MultiplexMixin) so all framework behaviour
(FIFO ordering, locality-aware routing, backoff, cache management,
probing) works identically.  The only difference: ``choose_replicas``
picks 2 consecutive replicas in round-robin order instead of 2 random.

With 64+ ingress replicas each starting from a random offset, the
aggregate distribution across LLM replicas is even.
"""

import logging
import random
from typing import List, Optional

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


class RandomRoundRobinRouter(FIFOMixin, LocalityMixin, MultiplexMixin, RequestRouter):
    """Routes requests to replicas in round-robin order.

    Identical to PowerOfTwoChoicesRequestRouter except for replica
    selection: instead of picking 2 random replicas, it picks 2
    consecutive replicas in a stable round-robin order.  The counter
    starts at a random offset so that different ingress instances
    don't all start at the same position.
    """

    def initialize_state(self, **kwargs):
        self._counter = random.randint(0, 2**31)

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        # Use the same locality / multiplex routing as power-of-two.
        # This also handles should_backoff signalling.
        if (
            pending_request is not None
            and pending_request.metadata.multiplexed_model_id
        ):
            candidate_replica_ids = self.apply_multiplex_routing(
                pending_request=pending_request,
            )
        else:
            candidate_replica_ids = self.apply_locality_routing(
                pending_request=pending_request,
            )

        if not candidate_replica_ids:
            return []

        # Sort for a stable ordering across calls.
        candidates = sorted(candidate_replica_ids, key=lambda rid: rid.unique_id)
        n = len(candidates)
        idx = self._counter % n
        self._counter += 1

        if n == 1:
            chosen_ids = [candidates[0]]
        else:
            # Two consecutive replicas in RR order (instead of 2 random).
            chosen_ids = [candidates[idx], candidates[(idx + 1) % n]]

        return [[self._replicas[chosen_id] for chosen_id in chosen_ids]]
