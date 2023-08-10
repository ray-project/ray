import logging
import threading
import time
import uuid
from typing import Dict, List, Union

import ray
from ray.anyscale.serve.centralized_scheduler import CentralSchedulerActor
from ray.serve._private.common import RunningReplicaInfo
from ray.serve._private.constants import SERVE_LOGGER_NAME
from ray.serve._private.router import (
    ActorReplicaWrapper,
    Query,
    ReplicaScheduler,
    ReplicaWrapper,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class MultiplexedLLMReplicaScheduler(ReplicaScheduler, threading.Thread):
    """Replica scheduler optimized for LLM multiplexing.

    Compared to the distributed two choices scheduler, this scheduler can make high
    quality scheduling decisions and enforce exact constraints. It does this by
    delegating all scheduling decisions to a singleton scheduling actor that holds
    all scheduling state.
    """

    def __init__(self, **kwargs):
        self._replicas: Dict[str, ReplicaWrapper] = {}
        self._router_uuid = uuid.uuid4().hex
        self._next_req_id = 0
        # TODO: make these scheduler options configurable.
        self._scheduler = CentralSchedulerActor.options(
            name="central_scheduler",
            get_if_exists=True,
        ).remote(
            max_replicas_per_model=1,
            max_models_per_replica=1,
            max_concurrent_requests_per_replica=2,
            model_keepalive_s=15,
        )
        self._submitted_requests: Dict[ray.ObjectRef, str] = {}
        super().__init__(daemon=True)
        self.start()

    async def assign_replica(
        self, query: Query
    ) -> Union[ray.ObjectRef, "ray._raylet.StreamingObjectRefGenerator"]:
        model_id = query.metadata.multiplexed_model_id
        if not model_id:
            raise ValueError(
                "This scheduler requires `multiplexed_model_id` to be specified."
            )
        request_id = f"{self._router_uuid}__{self._next_req_id}"
        self._next_req_id += 1
        replica_id = await self._scheduler.schedule.remote(request_id, model_id)
        logger.info(f"assigned {request_id} {model_id} to {replica_id}")

        replica = self._replicas[replica_id]
        res = replica.send_query(query)
        if hasattr(res, "_generator_ref"):
            # In the streaming response case, wait for the outer task.
            self._submitted_requests[res._generator_ref] = request_id
        else:
            self._submitted_requests[res] = request_id
        return res

    def update_running_replicas(self, running_replicas: List[RunningReplicaInfo]):
        self._replicas = {
            r.replica_tag: ActorReplicaWrapper(r) for r in running_replicas
        }
        ray.get(
            self._scheduler.update_running_replicas.remote(
                [r.replica_tag for r in running_replicas]
            )
        )

    def run(self):
        while True:
            try:
                while not self._submitted_requests:
                    time.sleep(0.1)
                done, _ = ray.wait(
                    list(self._submitted_requests), num_returns=1, timeout=0.1
                )
                for ref in done:
                    request_id = self._submitted_requests.pop(ref)
                    # TODO: implement fault handling on router failures.
                    ray.get(self._scheduler.notify_completed.remote(request_id))
            except Exception:
                logger.exception("Error polling for completed requests.")
