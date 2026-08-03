import asyncio
import logging
from typing import Optional, Set

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
from ray._private import ray_constants
from ray._private.gcs_pubsub import GcsAioJobSubscriber, GcsAioWorkerDeltaSubscriber
from ray.core.generated import (
    events_event_aggregator_service_pb2,
    gcs_pb2,
    gcs_service_pb2,
    gcs_service_pb2_grpc,
)
from ray.dashboard.modules.task_events.ray_event_converter import convert_to_task_events
from ray.dashboard.modules.task_events.task_event_storage import (
    GC_JOB_SUMMARY_INTERVAL_S,
    TaskEventStorage,
)
from ray.dashboard.subprocesses.module import SubprocessModule
from ray.dashboard.subprocesses.routes import SubprocessRouteTable as routes

logger = logging.getLogger(__name__)

# Max notifications drained from a GCS pubsub subscriber per poll.
_SUBSCRIBER_POLL_BATCH_SIZE = 100
# Delay before failing a dead worker's tasks, so in-flight FINISHED events can still land.
_MARK_FAILED_ON_WORKER_DEAD_DELAY_S = ray_constants.env_float(
    "RAY_DASHBOARD_TASK_EVENTS_MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 1.0
)
# Delay before failing a finished job's tasks, for the same reason.
_MARK_FAILED_ON_JOB_DONE_DELAY_S = ray_constants.env_float(
    "RAY_DASHBOARD_TASK_EVENTS_MARK_FAILED_ON_JOB_DONE_DELAY_S", 15.0
)


class TaskEventsHead(SubprocessModule):
    """Dashboard-head sink for task events and their reconciliation signals.

    Receives task events over HTTP from the per-node aggregator agents (which POST an
    ``AddEventsRequest`` payload) and stores them, and subscribes to GCS pubsub for the
    worker-death and job-finished notifications needed to reconcile task state. Everything
    is held in memory.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store = TaskEventStorage()
        self._background_tasks: Set[asyncio.Task] = set()
        self._worker_info_stub = None

    @classmethod
    def is_enabled(cls) -> bool:
        """Only load while the "task events out of GCS" migration is enabled; otherwise
        the module (and its GCS pubsub subscriptions) shouldn't run at all."""
        return ray._config.enable_task_events_to_dashboard_head()

    @property
    def num_task_events_stored(self) -> int:
        """Number of task attempts currently held in the store (for tests)."""
        return self._store.num_task_events_stored

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

        task_events, dropped_task_attempts = convert_to_task_events(add_events_request)
        self._store.record_data_loss_from_worker(dropped_task_attempts)
        for task_event in task_events:
            self._store.add_or_replace_task_event(task_event)
        logger.debug(
            "Received %d task events (%d attempts stored)",
            len(task_events),
            self._store.num_task_events_stored,
        )
        return dashboard_optional_utils.rest_response(
            status_code=dashboard_utils.HTTPStatusCode.OK,
            message="",
        )

    def _handle_worker_delta(self, worker_delta: gcs_pb2.WorkerDeltaData) -> None:
        # GCS_WORKER_DELTA_CHANNEL only carries failures, since it is published
        # only when GCS receives a worker failure event. So every message is a death.
        self._spawn(self._on_worker_dead(worker_delta.worker_id))

    def _handle_job_update(self, job_data: gcs_pb2.JobTableData) -> None:
        # GCS_JOB_CHANNEL fires on both job start and finish; only act on finished jobs.
        if job_data.is_dead:
            self._spawn(self._on_job_finished(job_data.job_id, job_data.end_time))

    async def _on_worker_dead(self, worker_id: bytes) -> None:
        # Fetch the dead worker's exit info concurrently with the delay.We should start
        # fetch immediately to avoid the worker info to be trimmed out of the table.
        # The delay lets in-flight FINISHED events land — so the two overlap instead
        # of adding up.
        # TODO(karticam): a (very unlikely) race — the record could be trimmed from that
        #   cache before this fetch completes.
        # TODO(karticam): avoid this extra round-trip by letting the worker-death
        #   subscription request the fields we need (e.g. exit info) in the notification.
        #   PR #64887 moves worker events to one-event framework. If we migrate worker
        #   events to dashboard head too, we can use that instead of pubsub, and we will
        #   also have full worker table data available to avoid the extra RPC.
        worker_table_data, _ = await asyncio.gather(
            self._get_worker_info(worker_id),
            asyncio.sleep(_MARK_FAILED_ON_WORKER_DEAD_DELAY_S),
        )
        if worker_table_data is None:
            logger.warning(
                f"No worker info found for dead worker {worker_id.hex()}; its tasks "
                "cannot be marked as failed."
            )
            return
        logger.debug(
            f"Marking all running tasks of worker {worker_id.hex()} as failed."
        )
        self._store.mark_tasks_failed_on_worker_dead(worker_id, worker_table_data)

    async def _on_job_finished(self, job_id: bytes, end_time_ms: int) -> None:
        # Delay so in-flight FINISHED events can still land before we fail the tasks.
        await asyncio.sleep(_MARK_FAILED_ON_JOB_DONE_DELAY_S)
        logger.info(f"Marking all running tasks of job {job_id.hex()} as failed.")
        self._store.mark_tasks_failed_on_job_ends(job_id, end_time_ms * 10**6)
        self._store.update_job_summary_on_job_done(job_id)

    async def _get_worker_info(
        self, worker_id: bytes
    ) -> Optional[gcs_pb2.WorkerTableData]:
        if self._worker_info_stub is None:
            self._worker_info_stub = gcs_service_pb2_grpc.WorkerInfoGcsServiceStub(
                self.aiogrpc_gcs_channel
            )
        reply = await self._worker_info_stub.GetWorkerInfo(
            gcs_service_pb2.GetWorkerInfoRequest(worker_id=worker_id)
        )
        if not reply.HasField("worker_table_data"):
            return None
        return reply.worker_table_data

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

    async def _gc_job_summary_loop(self) -> None:
        while True:
            await asyncio.sleep(GC_JOB_SUMMARY_INTERVAL_S)
            try:
                self._store.gc_job_summary()
            except Exception:
                logger.exception("Failed trimming task-event job summaries.")

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def run(self):
        await super().run()
        self._spawn(self._subscribe_for_worker_deaths())
        self._spawn(self._subscribe_for_finished_jobs())
        self._spawn(self._gc_job_summary_loop())
