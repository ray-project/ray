"""Round-robin request router.

RoundRobinRouter cycles through the candidate replicas passed in by Serve and
returns strictly ordered singleton ranks. Each request starts at the current
round-robin cursor; if that replica cannot fulfill the request, Serve tries the
next replica in order, wrapping around the candidate list.
"""

import random
from collections.abc import Sequence
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


class _RoundRobinReplicaRanks(Sequence[List[RunningReplica]]):
    """Lazy ordered singleton ranks for a strict round-robin attempt."""

    def __init__(
        self,
        replicas: List[RunningReplica],
        start_index: int,
    ):
        self._replicas = replicas
        self._start_index = start_index

    def __len__(self) -> int:
        return len(self._replicas)

    def __getitem__(self, index):
        if isinstance(index, slice):
            return list(self)[index]

        num_replicas = len(self._replicas)
        if index < 0:
            index += num_replicas
        if index < 0 or index >= num_replicas:
            raise IndexError(index)

        return [self._replicas[(self._start_index + index) % num_replicas]]

    def __iter__(self):
        num_replicas = len(self._replicas)
        for offset in range(num_replicas):
            yield [self._replicas[(self._start_index + offset) % num_replicas]]


class RoundRobinRouter(FIFOMixin, RequestRouter):
    """Routes requests by cycling through candidate replicas.

    Each call to ``choose_replicas`` advances a shared cursor by one position
    and returns ordered singleton ranks starting from that cursor. This upholds
    strict round-robin ordering while still allowing Serve's existing selector
    to continue to the next replica if the current one is full.
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
    ) -> Sequence[List[RunningReplica]]:
        if not candidate_replicas:
            return []

        if pending_request is not None:
            # Enable exponential-backoff sleep between outer retry iterations.
            # Without this, the base class tight-loops calling choose_replicas
            # when every replica is at capacity.
            pending_request.routing_context.should_backoff = True

        index = self._round_robin_counter % len(candidate_replicas)
        self._round_robin_counter += 1

        return _RoundRobinReplicaRanks(candidate_replicas, index)
