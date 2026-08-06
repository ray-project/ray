import asyncio
import logging
from typing import List, Optional, Set

import aiohttp.web

import ray
import ray.dashboard.optional_utils as dashboard_optional_utils
import ray.dashboard.utils as dashboard_utils
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
_MARK_FAILED_ON_WORKER_DEAD_DELAY_S = (
    ray._config.gcs_mark_task_failed_on_worker_dead_delay_ms() / 1000
)
# Delay before failing a finished job's tasks, for the same reason.
_MARK_FAILED_ON_JOB_DONE_DELAY_S = (
    ray._config.gcs_mark_task_failed_on_job_done_delay_ms() / 1000
)
# Timeout for the worker-table fetches used to reconcile dead workers.
_WORKER_INFO_FETCH_TIMEOUT_S = 30


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
                status_code=dashboard_utils.HTTPStatusCode.BAD_REQUEST,
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
        # Fetch the dead worker's exit info concurrently with the delay: we start the
        # fetch immediately so the record is less likely to be trimmed from the worker
        # table first, and the delay lets in-flight FINISHED events land, so the two
        # overlap instead of adding up. If the fetch comes back empty (record evicted, or
        # a transient failure), we still fail the tasks — just without exit details.
        # TODO(karticam): avoid this round-trip by having the worker-death subscription
        #   carry the fields we need. PR #64887 moves worker events to the one-event
        #   framework; migrating those to the dashboard head would give full worker data
        #   inline and drop this RPC.
        worker_table_data, _ = await asyncio.gather(
            self._get_worker_info(worker_id),
            asyncio.sleep(_MARK_FAILED_ON_WORKER_DEAD_DELAY_S),
        )
        if worker_table_data is None:
            logger.warning(
                f"Could not fetch exit info for dead worker {worker_id.hex()}; marking "
                "its tasks failed without exit details."
            )
        else:
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
        try:
            reply = await self._worker_info_stub.GetWorkerInfo(
                gcs_service_pb2.GetWorkerInfoRequest(worker_id=worker_id),
                timeout=_WORKER_INFO_FETCH_TIMEOUT_S,
            )
        except Exception as e:
            # A transient GCS/RPC failure must not sink the reconciliation task; treat it
            # like a missing record so the caller still fails the worker's tasks.
            logger.warning(f"Failed to fetch worker info for {worker_id.hex()}: {e}")
            return None
        if not reply.HasField("worker_table_data"):
            return None
        return reply.worker_table_data

    async def _get_all_worker_info(self) -> List[gcs_pb2.WorkerTableData]:
        if self._worker_info_stub is None:
            self._worker_info_stub = gcs_service_pb2_grpc.WorkerInfoGcsServiceStub(
                self.aiogrpc_gcs_channel
            )
        # Empty request → GCS returns every worker (no limit);
        reply = await self._worker_info_stub.GetAllWorkerInfo(
            gcs_service_pb2.GetAllWorkerInfoRequest(),
            timeout=_WORKER_INFO_FETCH_TIMEOUT_S,
        )
        return reply.worker_table_data

    async def _reconcile_dead_workers_on_startup(self) -> None:
        # GCS pubsub does not replay worker deaths from before we subscribed, so snapshot
        # the worker table once and fail tasks for any already-dead worker. This must run
        # AFTER subscribe() so a death between the snapshot and the subscription is still
        # delivered over pubsub.
        try:
            worker_infos = await self._get_all_worker_info()
        except Exception:
            logger.exception("Failed to reconcile dead workers on startup.")
            return
        for worker_table_data in worker_infos:
            if not worker_table_data.is_alive:
                self._store.mark_tasks_failed_on_worker_dead(
                    worker_table_data.worker_address.worker_id, worker_table_data
                )

    async def _subscribe_for_worker_deaths(self) -> None:
        subscriber = GcsAioWorkerDeltaSubscriber(address=self.gcs_address)
        await subscriber.subscribe()
        # Backfill deaths from before the subscription (pubsub won't replay them). Order
        # matters: subscribe first, then snapshot, so nothing falls between the two.
        # It happen that, since we already subscribed, we get a dead worker via pubsub
        # as well as through this route. But marking tasks dead failed is idempotent.
        await self._reconcile_dead_workers_on_startup()
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
