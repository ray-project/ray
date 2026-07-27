import asyncio
import collections
import logging
from typing import Set

import aiohttp.web

import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private.gcs_pubsub import GcsAioJobSubscriber, GcsAioWorkerDeltaSubscriber
from ray.core.generated import events_event_aggregator_service_pb2, gcs_pb2
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)

# Max notifications drained from a GCS pubsub subscriber per poll.
_SUBSCRIBER_POLL_BATCH_SIZE = 100


class TaskEventsHead(SubprocessModule):
    """Dashboard-head sink for task events and their GCS reconciliation signals.

    Receives task events over HTTP from the per-node aggregator agents (which POST an
    ``AddEventsRequest`` payload), and subscribes to GCS pubsub for the worker-death and
    job-finished notifications needed to reconcile task state (the signals
    ``GcsTaskManager`` consumes in-process today). Everything is held in memory for now.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # TODO(karticam): Replace these in-memory buffers with a task-event store +
        #   reconciliation logic (mirroring GcsTaskManager) that powers the state API.
        #   Consuming the worker-death / job-finished notifications below to actually
        #   fail tasks (MarkTasksFailedOnWorkerDead / MarkTasksFailedOnJobEnds) is a
        #   follow-up PR.
        self._events = collections.deque()
        self._dead_workers = collections.deque()
        self._finished_jobs = collections.deque()
        self._background_tasks: Set[asyncio.Task] = set()

    @property
    def num_events_received(self) -> int:
        """Number of task events currently held in the in-memory buffer (for tests)."""
        return len(self._events)

    @property
    def num_dead_workers_received(self) -> int:
        """Number of worker-death notifications received (for tests)."""
        return len(self._dead_workers)

    @property
    def num_finished_jobs_received(self) -> int:
        """Number of job-finished notifications received (for tests)."""
        return len(self._finished_jobs)

    def _deserialize_request(
        self, body: bytes
    ) -> events_event_aggregator_service_pb2.AddEventsRequest:
        """Deserialize the binary-proto POST body into an ``AddEventsRequest``."""
        return events_event_aggregator_service_pb2.AddEventsRequest.FromString(body)

    @routes.post("/api/task_events")
    async def add_task_events(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        body = await request.read()
        try:
            add_events_request = self._deserialize_request(body)
        except Exception as e:
            logger.warning(f"Failed to deserialize task events request: {e}")
            return dashboard_optional_utils.rest_response(
                status_code=dashboard_utils.HTTPStatusCode.INTERNAL_ERROR,
                message=f"Failed to deserialize task events request: {e}",
            )

        events_data = add_events_request.events_data
        self._events.extend(events_data.events)
        logger.debug(
            "Received %d task events (%d total buffered)",
            len(events_data.events),
            len(self._events),
        )
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
        )

    def _handle_worker_delta(self, worker_delta: gcs_pb2.WorkerDeltaData) -> None:
        """Record a worker-death notification. GCS_WORKER_DELTA_CHANNEL only carries
        failures, so every message is a death."""
        self._dead_workers.append(worker_delta)

    def _handle_job_update(self, job_data: gcs_pb2.JobTableData) -> None:
        """Record a job-finished notification. GCS_JOB_CHANNEL fires on both job start and
        finish; keep only finished jobs (``is_dead``) to mirror ``OnJobFinished``."""
        if job_data.is_dead:
            self._finished_jobs.append(job_data)

    async def _subscribe_for_worker_deaths(self) -> None:
        subscriber = GcsAioWorkerDeltaSubscriber(address=self.gcs_address)
        await subscriber.subscribe()
        while True:
            try:
                for _, worker_delta in await subscriber.poll(
                    batch_size=_SUBSCRIBER_POLL_BATCH_SIZE
                ):
                    self._handle_worker_delta(worker_delta)
            except Exception:
                logger.exception("Failed handling worker-death notifications.")

    async def _subscribe_for_finished_jobs(self) -> None:
        subscriber = GcsAioJobSubscriber(address=self.gcs_address)
        await subscriber.subscribe()
        while True:
            try:
                for _, job_data in await subscriber.poll(
                    batch_size=_SUBSCRIBER_POLL_BATCH_SIZE
                ):
                    self._handle_job_update(job_data)
            except Exception:
                logger.exception("Failed handling job-finished notifications.")

    async def run(self):
        await super().run()
        for coro in (
            self._subscribe_for_worker_deaths(),
            self._subscribe_for_finished_jobs(),
        ):
            task = asyncio.create_task(coro)
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
