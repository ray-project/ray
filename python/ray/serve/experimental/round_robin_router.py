"""Round-robin request router.

RoundRobinRouter cycles through the candidate replicas passed in by Serve and
returns the selected replica as the highest-ranked candidate for each request.
If that replica is unavailable or at capacity, the existing Serve retry loop
calls back into the router and advances to the next replica.
"""

import random
from typing import List, Optional

from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    FIFOMixin,
    RequestRouter,
)


class RoundRobinRouter(FIFOMixin, RequestRouter):
    """Routes requests by cycling through candidate replicas.

    Each call to ``choose_replicas`` advances a shared cursor by one position
    and returns a single highest-ranked candidate. This keeps the normal
    queue-length, rejection, retry, and backoff machinery in charge after the
    round-robin pick is made.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._round_robin_counter = random.randrange(2**31)

    def initialize_state(self, **kwargs) -> None:
        pass

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> List[List[RunningReplica]]:
        if not candidate_replicas:
            return []

        index = self._round_robin_counter % len(candidate_replicas)
        self._round_robin_counter += 1
        return [[candidate_replicas[index]]]
