import asyncio
import logging
import random
from typing import (
    AsyncGenerator,
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
    LocalityScheduleMixin,
    MultiplexScheduleMixin,
    ReplicaScheduler,
)
from ray.serve._private.replica_scheduler.replica_wrapper import (
    RunningReplica,
)

logger = logging.getLogger(SERVE_LOGGER_NAME)


class PowerOfTwoChoicesReplicaScheduler(
    MultiplexScheduleMixin, LocalityScheduleMixin, ReplicaScheduler
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
        pending_request: Optional[PendingRequest] = None,
    ) -> AsyncGenerator[List[RunningReplica], None]:
        """Generator that repeatedly chooses (at most) two random available replicas.

        In the first iteration, only replicas colocated on the same node as this router
        will be considered. If those are occupied, the full set of replicas will be
        considered on subsequent iterations.

        For multiplexing, this will first attempt to choose replicas that have the
        requested model ID for a configured timeout. If no replicas with the matching
        model ID are available after that timeout, it will fall back to the regular
        procedure.

        After each iteration, there will be an increasing backoff sleep time (dictated
        by `self.backoff_sequence_s`). The caller should exit the generator to reset the
        backoff sleep time.
        """
        entered_backoff = False
        try:
            backoff_index = 0

            while True:
                # If no replicas are available, wait until `update_replicas` is called.
                while len(self._replicas) == 0:
                    logger.info(
                        "No replicas are currently available for "
                        f"{self._deployment_id}.",
                        extra={"log_to_stderr": False},
                    )
                    self._replicas_updated_event.clear()
                    await self._replicas_updated_event.wait()
                    logger.info(
                        f"New replicas are available for {self._deployment_id}, "
                        "attempting to schedule queued requests.",
                        extra={"log_to_stderr": False},
                    )

                if (
                    pending_request is not None
                    and pending_request.metadata.multiplexed_model_id
                ):
                    # Get candidates for multiplexed model ID.
                    candidate_replica_ids = self.apply_multiplex_scheduling(
                        pending_request=pending_request,
                    )
                    # print(f"after calling apply_multiplex_scheduling {pending_request=} {candidate_replica_ids=}")
                else:
                    # Get candidates for locality preference.
                    candidate_replica_ids = self.apply_locality_scheduling(
                        pending_request=pending_request,
                    )

                # print(f"in choose_replicas {candidate_replica_ids=}")
                if candidate_replica_ids:
                    chosen_ids = random.sample(
                        list(candidate_replica_ids),
                        k=min(2, len(candidate_replica_ids)),
                    )
                    # print(f"in candidate_replica_ids loop {chosen_ids=}")
                    yield [self._replicas[chosen_id] for chosen_id in chosen_ids]

                # We have a slight unintended behavior when enabled locality routing
                # for both node and AZ. The intention is to try same node first,
                # then try same AZ if node fails, then try everything else until a
                # replica is found. These sequence should only help to reduce the
                # latency of the request. No backoff and sleep should be applied, until
                # we have fall into the case trying on all available replicas.
                if not pending_request.scheduling_context.should_backoff:
                    continue

                if not entered_backoff:
                    entered_backoff = True
                    self.num_scheduling_tasks_in_backoff += 1
                    self.num_scheduling_tasks_in_backoff_gauge.set(
                        self.num_scheduling_tasks_in_backoff
                    )

                await asyncio.sleep(self.backoff_sequence_s[backoff_index])
                backoff_index = min(backoff_index + 1, len(self.backoff_sequence_s) - 1)
        finally:
            if entered_backoff:
                self.num_scheduling_tasks_in_backoff -= 1
                self.num_scheduling_tasks_in_backoff_gauge.set(
                    self.num_scheduling_tasks_in_backoff
                )
