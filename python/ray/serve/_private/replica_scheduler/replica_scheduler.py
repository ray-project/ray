import enum
import math
import random
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import (
    DefaultDict,
    Dict,
    List,
    Optional,
    Set,
)

from ray.serve._private.common import ReplicaID, RunningReplicaInfo
from ray.serve._private.constants import (
    RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
)
from ray.serve._private.replica_scheduler.common import (
    PendingRequest,
    ReplicaQueueLengthCache,
)
from ray.serve._private.replica_scheduler.replica_wrapper import RunningReplica


class LocalityScope(str, enum.Enum):
    NODE = "NODE"
    AVAILABILITY_ZONE = "AVAILABILITY_ZONE"


class LocalityScheduleMixin:
    def __init__(
        self,
        prefer_local_node_routing: bool = False,
        prefer_local_az_routing: bool = False,
        self_availability_zone: Optional[str] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._prefer_local_node_routing = prefer_local_node_routing
        self._prefer_local_az_routing = prefer_local_az_routing
        self._self_availability_zone = self_availability_zone

        # Colocated replicas (e.g. wrt node, AZ)
        self._colocated_replica_ids: DefaultDict[
            LocalityScope, Set[ReplicaID]
        ] = defaultdict(set)

    def discard_colocated_replica_ids_on_replica_actor_died(
        self, replica_id: ReplicaID
    ):
        for id_set in self._colocated_replica_ids.values():
            id_set.discard(replica_id)

    def update_discard_colocated_replica_ids_with_replicas(
        self, replicas: List[RunningReplica]
    ):
        # print(
        #     f"in update_discard_colocated_replica_ids_with_replicas {self._colocated_replica_ids=} {[(r.node_id, r.availability_zone) for r in replicas]=} {self._self_node_id=} {self._self_availability_zone=}"
        # )
        new_colocated_replica_ids = defaultdict(set)

        for r in replicas:
            if self._self_node_id is not None and r.node_id == self._self_node_id:
                new_colocated_replica_ids[LocalityScope.NODE].add(r.replica_id)
            if (
                self._self_availability_zone is not None
                and r.availability_zone == self._self_availability_zone
            ):
                new_colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE].add(
                    r.replica_id
                )

        self._colocated_replica_ids = new_colocated_replica_ids

    def apply_locality_scheduling(
        self,
        pending_request: Optional[PendingRequest] = None,
    ) -> Set[ReplicaID]:
        # print(
        #     f"in apply_locality_scheduling {self._colocated_replica_ids=} {self._replica_id_set=}"
        # )
        if not pending_request:
            return self._replica_id_set

        if (
            self._prefer_local_node_routing
            and not pending_request.scheduling_context.tried_same_node
            and len(self._colocated_replica_ids[LocalityScope.NODE]) > 0
        ):
            # Attempt to schedule requests to replicas on the
            # same node at most once
            candidate_replica_ids = self._colocated_replica_ids[LocalityScope.NODE]
            # print(f"_prefer_local_node_routing {candidate_replica_ids=}")
            pending_request.scheduling_context.tried_same_node = True
            pending_request.scheduling_context.should_backoff = False
        elif (
            self._prefer_local_az_routing
            and not pending_request.scheduling_context.tried_same_az
            and len(self._colocated_replica_ids[LocalityScope.AVAILABILITY_ZONE]) > 0
        ):
            # Attempt to schedule requests to replicas in the same
            # AZ at most once
            candidate_replica_ids = self._colocated_replica_ids[
                LocalityScope.AVAILABILITY_ZONE
            ]
            pending_request.scheduling_context.tried_same_az = True
            pending_request.scheduling_context.should_backoff = False
        else:
            # On subsequent iterations or when there are no replicas on the same
            # node or AZ, consider all available replicas.
            candidate_replica_ids = self._replica_id_set
            pending_request.scheduling_context.should_backoff = True
        # print(f"locality decision {candidate_replica_ids=}")
        return candidate_replica_ids


class MultiplexScheduleMixin:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._multiplexed_model_id_to_replica_ids: DefaultDict[
            str, Set[ReplicaID]
        ] = defaultdict(set)

        # When there is no match for a multiplexed model id, we will try to fall back
        # to all replicas immediately. This set is used to make sure we only fall back
        # once for concurrent requests for the same model id.
        # Whenever there is a match, we will remove the model id from this set.
        self._multiplexed_model_id_fallback_match: Set[str] = set()

    def update_multiplexed_model_ids_with_replicas(
        self, replicas: List[RunningReplica]
    ):
        new_multiplexed_model_id_to_replica_ids = defaultdict(set)

        for r in replicas:
            for model_id in r.multiplexed_model_ids:
                new_multiplexed_model_id_to_replica_ids[model_id].add(r.replica_id)

        self._multiplexed_model_id_to_replica_ids = (
            new_multiplexed_model_id_to_replica_ids
        )
        # print(f"in update_multiplexed_model_ids_with_replicas {self._multiplexed_model_id_to_replica_ids=} {replicas=}")

    def _get_replica_ids_with_fewest_multiplexed_models(self) -> Set[str]:
        """Get the set of replicas that have the fewest multiplexed models loaded."""
        candidates = set()
        sorted_replicas = sorted(
            self._replicas.values(), key=lambda x: len(x.multiplexed_model_ids)
        )
        least_num_multiplexed_model_ids = math.inf
        for replica in sorted_replicas:
            if len(replica.multiplexed_model_ids) <= least_num_multiplexed_model_ids:
                candidates.add(replica.replica_id)
                least_num_multiplexed_model_ids = len(replica.multiplexed_model_ids)
            else:
                break

        # print(f"in _get_replica_ids_with_fewest_multiplexed_models {candidates=}")
        return candidates

    @property
    def multiplexed_matching_timeout(self) -> float:
        return random.uniform(
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S,
            RAY_SERVE_MULTIPLEXED_MODEL_ID_MATCHING_TIMEOUT_S * 2,
        )

    def apply_multiplex_scheduling(
        self,
        pending_request: PendingRequest,
    ) -> Set[ReplicaID]:
        # TODO (genesu): add doc string and data structure for the return type
        if not pending_request.scheduling_context.multiplexed_start_matching_time:
            pending_request.scheduling_context.multiplexed_start_matching_time = (
                time.time()
            )

        multiplexed_start_matching_time = (
            pending_request.scheduling_context.multiplexed_start_matching_time
        )
        multiplexed_model_id = pending_request.metadata.multiplexed_model_id
        if (
            time.time() - multiplexed_start_matching_time
            < self.multiplexed_matching_timeout
        ):
            # print(f"in A {time.time() - multiplexed_start_matching_time=} {self.multiplexed_matching_timeout=}")
            candidate_replica_ids = self._multiplexed_model_id_to_replica_ids.get(
                multiplexed_model_id, None
            )
            if (
                not candidate_replica_ids
                and multiplexed_model_id
                not in self._multiplexed_model_id_fallback_match
            ) or pending_request.scheduling_context.tried_first_multiplexed_models:
                # print(f"in B")
                # When there is no match for a multiplexed model id
                # or when the replica(s) with the matching model id is busy,
                # first try to fall back to replicas with the fewest models.
                candidate_replica_ids = (
                    self._get_replica_ids_with_fewest_multiplexed_models()
                )
                self._multiplexed_model_id_fallback_match.add(multiplexed_model_id)
            elif candidate_replica_ids:
                # print(f"in C")
                self._multiplexed_model_id_fallback_match.discard(multiplexed_model_id)
            pending_request.scheduling_context.tried_first_multiplexed_models = True
        elif not pending_request.scheduling_context.tried_fewest_multiplexed_models:
            # After the `multiplexed_matching_timeout` is up, first try
            # routing to replicas that have the fewest models loaded.
            # We only try this once to avoid deterministically retrying on
            # the same replicas repeatedly.
            # print(f"in D")
            candidate_replica_ids = (
                self._get_replica_ids_with_fewest_multiplexed_models()
            )
            pending_request.scheduling_context.tried_fewest_multiplexed_models = True
        else:
            # print(f"in F")
            # If the timeout is up, and we've already tried the candidates
            # with the fewest models loaded, fall back to all replicas.
            candidate_replica_ids = self._replica_id_set

        pending_request.scheduling_context.should_backoff = True
        return candidate_replica_ids


class ReplicaScheduler(ABC):
    """Abstract interface for a replica scheduler (how the router calls it)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    async def choose_replica_for_request(
        self, pending_request: PendingRequest, *, is_retry: bool = False
    ) -> RunningReplica:
        pass

    @abstractmethod
    def create_replica_wrapper(
        self, replica_info: RunningReplicaInfo
    ) -> RunningReplica:
        pass

    @abstractmethod
    def update_replicas(self, replicas: List[RunningReplica]):
        pass

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        """Compatibility shim for RunningReplicaInfo datatype."""
        return self.update_replicas(
            [self.create_replica_wrapper(r) for r in running_replicas]
        )

    @abstractmethod
    def on_replica_actor_died(self, replica_id: ReplicaID):
        pass

    @abstractmethod
    def on_replica_actor_unavailable(self, replica_id: ReplicaID):
        pass

    @property
    @abstractmethod
    def replica_queue_len_cache(self) -> ReplicaQueueLengthCache:
        pass

    @property
    @abstractmethod
    def curr_replicas(self) -> Dict[ReplicaID, RunningReplica]:
        pass
