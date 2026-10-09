"""Round-robin request router.

RoundRobinRouter cycles through the candidate replicas passed in by Serve and
returns strictly ordered singleton ranks. Each request starts at the current
round-robin cursor; if that replica cannot fulfill the request, Serve tries the
next replica in order, wrapping around the candidate list.
"""

import random
from collections.abc import Sequence
from typing import Dict, List, Optional

from ray.serve._private.request_router.common import (
    PendingRequest,
)
from ray.serve._private.request_router.replica_wrapper import (
    RunningReplica,
)
from ray.serve._private.request_router.request_router import (
    FIFOMixin,
    MultiplexMixin,
    RequestRouter,
)

_MAX_ROUND_ROBIN_COUNTER = 2**31


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


class RoundRobinRouter(FIFOMixin, MultiplexMixin, RequestRouter):
    """Routes requests by cycling through candidate replicas.

    Non-multiplexed requests and multiplexing fallbacks advance a shared cursor.
    Each multiplexed model advances its own cursor only when routing to replicas
    that already host that model, so requests to other models do not perturb
    round-robin ordering among warm replicas. The router returns ordered
    singleton ranks starting from the selected cursor, allowing Serve's existing
    selector to continue to the next replica if the current one is full.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._round_robin_counter = random.randrange(_MAX_ROUND_ROBIN_COUNTER)
        self._multiplexed_round_robin_counters: Dict[str, int] = {}

    def initialize_state(self, **kwargs) -> None:
        pass

    def _update_multiplexed_model_ids_with_replicas(
        self, replicas: List[RunningReplica]
    ) -> None:
        super()._update_multiplexed_model_ids_with_replicas(replicas)
        self._multiplexed_round_robin_counters = {
            model_id: counter
            for model_id, counter in self._multiplexed_round_robin_counters.items()
            if model_id in self._multiplexed_model_id_to_replica_ids
        }

    async def choose_replicas(
        self,
        candidate_replicas: List[RunningReplica],
        pending_request: Optional[PendingRequest] = None,
    ) -> Sequence[List[RunningReplica]]:
        multiplexed_model_id: Optional[str] = None
        routing_to_matching_model_replicas = False
        if pending_request is not None:
            # Enable exponential-backoff sleep between outer retry iterations.
            # Without this, the base class tight-loops calling choose_replicas
            # when every replica is at capacity.
            pending_request.routing_context.should_backoff = True

            if pending_request.metadata.multiplexed_model_id:
                multiplexed_model_id = pending_request.metadata.multiplexed_model_id
                is_first_multiplexed_attempt = (
                    not pending_request.routing_context.tried_first_multiplexed_models
                )
                candidate_replica_ids = self.apply_multiplex_routing(pending_request)
                matching_model_replica_ids = (
                    self._multiplexed_model_id_to_replica_ids.get(
                        multiplexed_model_id, set()
                    )
                )
                routing_to_matching_model_replicas = (
                    is_first_multiplexed_attempt
                    and not pending_request.routing_context.tried_fewest_multiplexed_models
                    and bool(matching_model_replica_ids)
                    and candidate_replica_ids == matching_model_replica_ids
                )
                candidate_replicas = [
                    replica
                    for replica in candidate_replicas
                    if replica.replica_id in candidate_replica_ids
                ]

        if not candidate_replicas:
            return []

        if routing_to_matching_model_replicas:
            assert multiplexed_model_id is not None
            counter = self._multiplexed_round_robin_counters.get(multiplexed_model_id)
            if counter is None:
                counter = random.randrange(_MAX_ROUND_ROBIN_COUNTER)
            index = counter % len(candidate_replicas)
            self._multiplexed_round_robin_counters[multiplexed_model_id] = counter + 1
        else:
            index = self._round_robin_counter % len(candidate_replicas)
            self._round_robin_counter += 1

        return _RoundRobinReplicaRanks(candidate_replicas, index)
