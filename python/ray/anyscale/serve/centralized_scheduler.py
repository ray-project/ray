"""A centralized scheduler that implements model multiplexing with hard constraints.

Constraints include:
    - max number of models per replica
    - max number of replicas per model
    - minimum delay before unloading a loaded model
    - max concurrency per replica

Since the scheduler is centralized as a singleton named actor, it can consistently
enforce these constraints for robust scheduling of multiplexed workloads.
"""

import asyncio
import logging
import time
from abc import ABC
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, DefaultDict, Dict, List, Optional, Tuple, Union

import ray
from ray._private.utils import run_background_task
from ray.serve._private.constants import SERVE_LOGGER_NAME

# Type aliases for id types.
ReplicaID = str
ModelID = str
RequestID = str
RouterID = str

logger = logging.getLogger(SERVE_LOGGER_NAME)


@dataclass
class ModelState:
    """Scheduling state of a loaded model on a replica.

    Attributes:
        load_time: time when the model was initially loaded.
        num_active_requests: current requests running on this model.
    """

    load_time: int
    num_active_requests: int = 0


@dataclass
class ReplicaState:
    """Scheduling state of a live replica.

    Attributes:
        models: currently loaded models on this replica by model id.
    """

    models: Dict[ModelID, ModelState] = field(default_factory=dict)


@dataclass
class SchedulerState:
    """Multiplexed scheduling state of an entire deployment.

    Attributes:
        replicas: live replicas by replica id.
        pending_requests: index of live requests by request id.
        router_requests: index of live requests by router id.
    """

    replicas: Dict[ReplicaID, ReplicaState] = field(default_factory=dict)

    pending_requests: Dict[RequestID, Tuple[ModelID, ReplicaID]] = field(
        default_factory=dict
    )
    router_requests: DefaultDict[RouterID, List[RequestID]] = field(
        default_factory=lambda: defaultdict(list)
    )

    def complete_pending_requests(self, routers: List[RouterID]) -> None:
        """Mark all pending requests finished by router id.

        Args:
            routers: target routers to finish requests.
        """
        for router_id in routers:
            for request_id in self.router_requests.get(router_id, []):
                self.complete_request(request_id)
            self.router_requests.pop(router_id, None)

    def update_replicas(self, replica_ids: List[ReplicaID]) -> None:
        """Set live replicas, removing the state of dead replicas.

        Args:
            replica_ids: list of currently live replicas.
        """
        for replica_id in list(self.replicas.keys()):
            if replica_id not in replica_ids:
                del self.replicas[replica_id]
                logger.info(f"Removing replica {replica_id}")
        for replica_id in replica_ids:
            if replica_id not in self.replicas:
                self.replicas[replica_id] = ReplicaState()
                logger.info(f"Adding replica {replica_id}")

    def assign_request(
        self,
        request_id: RequestID,
        model_id: ModelID,
        replica_id: ReplicaID,
        router_id: RouterID,
        current_time: int,
        model_to_evict: Optional[ModelID] = None,
    ) -> None:
        """Associate a request with a particular replica in the scheduling state.

        This does not actually dispatch the request, that part is delegated to the
        router handling the request.

        Args:
            request_id: unique id for the request.
            model_id: model id for the request.
            replica_id: replica the request should be assigned to.
            current_time: timestamp of the request.
            model_to_evict: model to disassociate from the given replica to make room
                for this rquest.
        """
        self.pending_requests[request_id] = (model_id, replica_id)
        self.router_requests[router_id].append(request_id)
        if model_to_evict:
            del self.replicas[replica_id].models[model_to_evict]
        if model_id not in self.replicas[replica_id].models:
            self.replicas[replica_id].models[model_id] = ModelState(current_time)
        self.replicas[replica_id].models[model_id].num_active_requests += 1

    def complete_request(self, request_id: RequestID) -> None:
        """Mark a request as completed in the scheduling state.

        This should be called by routers when their request completes.

        Args:
            request_id: unique id for the request.
        """
        model_id, replica_id = self.pending_requests[request_id]
        if replica_id in self.replicas:
            replica = self.replicas[replica_id]
            if model_id in replica.models:
                replica.models[model_id].num_active_requests -= 1

    def find_schedulable_replica(
        self, model_id: ModelID, max_concurrent: int
    ) -> Optional[ReplicaID]:
        """Find a replica that can immediately handle this request.

        Args:
            model_id: the model required by the request.
            max_concurrent: the max number of concurrent requests to allow per replica.
        """
        for r_id, replica_state in self.replicas.items():
            for m_id, model_state in replica_state.models.items():
                if (
                    m_id == model_id
                    and model_state.num_active_requests < max_concurrent
                ):
                    return r_id

        return None

    def find_evictable_replica(
        self,
        model_id: ModelID,
        max_models_per_replica: int,
        model_keepalive_s: int,
        current_time: int,
    ) -> (Optional[str], Optional[str]):
        """Find a replica that can load the given model, subject to constraints.

        Args:
            model_id: the model required by the request
            max_models_per_replica: max number of models to allow per replica.
            model_keepalive_s: time delay required before a model is eligible to be
                unloaded.
            current_time: timestamp.

        Returns:
            Tuple of replica id where the model will be loaded, if possible, and the
            model id to evict, if any.
        """
        horizon = current_time - model_keepalive_s

        # Prefer scheduling on empty slots.
        for r_id, replica_state in self.replicas.items():
            if len(replica_state.models) < max_models_per_replica:
                return r_id, None

        # Next try to schedule by evicting an existing model.
        for r_id, replica_state in self.replicas.items():
            for m_id, model_state in replica_state.models.items():
                if model_state.load_time < horizon and m_id != model_id:
                    return r_id, m_id

        return None, None

    # TODO: add a reverse index to speed this up.
    def count_replicas_for_model(self, model_id: ModelID) -> int:
        """Return the number of replicas hosting the given model.

        Args:
            model_id: the model to query.
        """
        count = 0
        for r_id, replica_state in self.replicas.items():
            for m_id, model_state in replica_state.models.items():
                if m_id == model_id:
                    count += 1
        return count

    def debug_dict(self) -> dict:
        """Return a dict snapshot of this object for testing purposes."""
        return {
            replica_id: {
                model_id: (model_state.load_time, model_state.num_active_requests)
                for model_id, model_state in replica_state.models.items()
            }
            for replica_id, replica_state in self.replicas.items()
        }


class CentralSchedulerInterface(ABC):
    def try_schedule(self, request_id: str, model_id: str) -> Optional[str]:
        """Try to schedule a request from a router for the given model id.

        Args:
            request_id: Unique id for the request.
            model_id: The multiplexed model id of the query. An error is raised if
                this is not provided.

        Returns:
            replica id the query should be sent to, if schedulable.
        """

    def update_running_replicas(self, running_replicas: List[str]) -> None:
        """Update the list of live replicas.

        Args:
            running_replicas: Ids of currently live replicas.
        """

    def notify_completed(self, request_id: str) -> None:
        """Notify a request is completed.

        Args:
            request_id: The request that is completed.
        """


class SyncCentralScheduler(CentralSchedulerInterface):
    """Sync local impl for unit testing."""

    def __init__(
        self,
        max_replicas_per_model: int,
        max_models_per_replica: int,
        max_concurrent_requests_per_replica: int,
        model_keepalive_s: int,
        clock: Callable[[], int] = time.time,
    ):
        self.max_replicas_per_model = max_replicas_per_model
        self.max_models_per_replica = max_models_per_replica
        self.max_concurrent_requests_per_replica = max_concurrent_requests_per_replica
        self.model_keepalive_s = model_keepalive_s
        self.state = SchedulerState()
        self.clock = clock
        logger.info(f"SyncCentralScheduler({self.__dict__}) created")

    def try_schedule(
        self, request_id: str, model_id: str, router_id: str
    ) -> Optional[str]:
        replica_id = self.state.find_schedulable_replica(
            model_id, self.max_concurrent_requests_per_replica
        )
        if replica_id:
            self.state.assign_request(
                request_id, model_id, replica_id, router_id, self.clock()
            )
            logger.debug(
                f"{request_id} Scheduled successfully on {replica_id} {model_id}: "
                f"{self.state.debug_dict()}"
            )
            return replica_id

        if self.state.count_replicas_for_model(model_id) >= self.max_replicas_per_model:
            return None

        replica_id, model_to_evict = self.state.find_evictable_replica(
            model_id, self.max_models_per_replica, self.model_keepalive_s, self.clock()
        )
        if replica_id:
            self.state.assign_request(
                request_id,
                model_id,
                replica_id,
                router_id,
                self.clock(),
                model_to_evict=model_to_evict,
            )
            logger.debug(
                f"{request_id} Scheduled successfully on {replica_id} {model_id}: "
                f"{self.state.debug_dict()}, after evicting {model_to_evict}"
            )
            return replica_id

        return None

    def update_running_replicas(self, running_replicas: List[str]) -> None:
        self.state.update_replicas(running_replicas)

    def notify_completed(self, request_id: str) -> None:
        if request_id in self.state.pending_requests:
            self.state.complete_request(request_id)
        logger.debug(f"Request completed: {request_id}: {self.state.debug_dict()}")

    def clean_up_scheduler_state(self, routers: List[RouterID]):
        self.state.complete_pending_requests(routers)


@ray.remote(max_restarts=-1, max_task_retries=3)
class CentralSchedulerActor(SyncCentralScheduler):
    """Remote impl for use by Serve router using the LLM multiplex scheduler."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.routers_health_ref: Dict[str, Union[ray.ObjectRef, None]] = {}
        run_background_task(self.check_router_health())

    async def check_router_health(self):
        while True:
            for router_id in list(self.routers_health_ref.keys()):
                health_ref = self.routers_health_ref[router_id]
                try:
                    if health_ref:
                        done, _ = await asyncio.wait([health_ref])
                        if done and done.pop().exception():
                            logger.warning(f"Mark router {router_id} dead.")
                            self.clean_up_scheduler_state([router_id])
                            self.routers_health_ref.pop(router_id, None)
                except Exception as e:
                    logger.warning(
                        f"Unexpected error {e} when checking "
                        f"router {router_id} health."
                    )
            await asyncio.sleep(0.5)

    async def schedule(
        self,
        request_id: str,
        model_id: str,
        router_id: str,
        health_ref: List[ray.ObjectRef],
    ) -> str:
        # health_ref is passed in to make sure if the CentralSchedulerActor is
        # restarted, the health ref can be recollected from routers.
        if router_id not in self.routers_health_ref:
            self.routers_health_ref[router_id] = health_ref[0]

        res = self.try_schedule(request_id, model_id, router_id)
        while not res:
            await asyncio.sleep(0.1)
            res = self.try_schedule(request_id, model_id, router_id)
        return res
