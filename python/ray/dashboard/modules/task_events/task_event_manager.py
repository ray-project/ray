import asyncio
import logging
from typing import Optional, Set

import ray
from ray._private.gcs_pubsub import GcsAioJobSubscriber, GcsAioWorkerDeltaSubscriber
from ray.core.generated import gcs_pb2, gcs_service_pb2, gcs_service_pb2_grpc
from ray.dashboard.modules.task_events.task_event_storage import (
    GC_JOB_SUMMARY_INTERVAL_S,
    TaskEventStorage,
)

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


class TaskEventManager:
    """Owns the background upkeep of the in-memory task-event store.

    This is where GcsTaskManager's non-RPC background work lands in the "task events out of
    GCS" migration. It subscribes to GCS pubsub and, after a short delay that lets in-flight
    FINISHED events land, marks a dead worker's / finished job's running tasks failed
    (reading the dead worker's exit info from the GCS worker table), and periodically GCs the
    store's per-job summaries. Kept out of the dashboard-head module so that file only defines
    the external HTTP API; the head constructs a manager and calls ``start`` on its event
    loop to register these loops.
    """

    def __init__(
        self,
        store: TaskEventStorage,
        gcs_address: str,
        gcs_aio_channel,
    ):
        self._store = store
        self._gcs_address = gcs_address
        self._gcs_aio_channel = gcs_aio_channel
        self._background_tasks: Set[asyncio.Task] = set()
        self._worker_info_stub = None

    def start(self) -> None:
        """Spawn the subscription and GC loops on the running event loop."""
        self._spawn(self._subscribe_for_worker_deaths())
        self._spawn(self._subscribe_for_finished_jobs())
        self._spawn(self._gc_job_summary_loop())

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
                self._gcs_aio_channel
            )
        reply = await self._worker_info_stub.GetWorkerInfo(
            gcs_service_pb2.GetWorkerInfoRequest(worker_id=worker_id)
        )
        if not reply.HasField("worker_table_data"):
            return None
        return reply.worker_table_data

    async def _subscribe_for_worker_deaths(self) -> None:
        subscriber = GcsAioWorkerDeltaSubscriber(address=self._gcs_address)
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
        subscriber = GcsAioJobSubscriber(address=self._gcs_address)
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
