import asyncio
import logging
from typing import List, Optional, Set

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
# Timeout for the GCS table fetches used to reconcile dead workers and finished jobs.
_GCS_INFO_FETCH_TIMEOUT_S = 30


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
        self._job_info_stub = None

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
        # Fetch the dead worker's exit info concurrently with the delay: we start the
        # fetch immediately so the record is less likely to be trimmed from the worker
        # table first, and the delay lets in-flight FINISHED events land, so the two
        # overlap instead of adding up. If the fetch comes back empty (record evicted, or
        # a transient failure), we still fail the tasks — just without exit details.
        # TODO(karticam): avoid this round-trip by having the worker-death subscription
        #   carry the fields we need. PR #64887 moves worker events to the one-event
        #   framework; migrating those to the dashboard head would give full worker data
        #   inline (like node_head, which acts on the subscription payload) and drop this RPC.
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
                self._gcs_aio_channel
            )
        try:
            reply = await self._worker_info_stub.GetWorkerInfo(
                gcs_service_pb2.GetWorkerInfoRequest(worker_id=worker_id),
                timeout=_GCS_INFO_FETCH_TIMEOUT_S,
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
                self._gcs_aio_channel
            )
        # Empty request → GCS returns every worker (no limit); the is_alive filter can't
        # select dead-only server-side (only `true` is respected), so the caller filters.
        reply = await self._worker_info_stub.GetAllWorkerInfo(
            gcs_service_pb2.GetAllWorkerInfoRequest(),
            timeout=_GCS_INFO_FETCH_TIMEOUT_S,
        )
        return reply.worker_table_data

    async def _get_all_job_info(self) -> List[gcs_pb2.JobTableData]:
        if self._job_info_stub is None:
            self._job_info_stub = gcs_service_pb2_grpc.JobInfoGcsServiceStub(
                self._gcs_aio_channel
            )
        # Empty request → GCS returns every job (no limit).
        reply = await self._job_info_stub.GetAllJobInfo(
            gcs_service_pb2.GetAllJobInfoRequest(),
            timeout=_GCS_INFO_FETCH_TIMEOUT_S,
        )
        return reply.job_info_list

    async def _reconcile_dead_workers_on_startup(self) -> None:
        # GCS pubsub does not replay worker deaths from before we subscribed, so snapshot
        # the worker table once and fail tasks for any already-dead worker. This must run
        # AFTER subscribe() so a death between the snapshot and the subscription is still
        # delivered over pubsub — no gap (see node_head._subscribe_for_node_updates).
        try:
            # Overlap the snapshot fetch with the same delay _on_worker_dead uses, so
            # in-flight FINISHED events can land before we fail the tasks.
            worker_infos, _ = await asyncio.gather(
                self._get_all_worker_info(),
                asyncio.sleep(_MARK_FAILED_ON_WORKER_DEAD_DELAY_S),
            )
        except Exception:
            logger.exception("Failed to reconcile dead workers on startup.")
            return
        for worker_table_data in worker_infos:
            if not worker_table_data.is_alive:
                self._store.mark_tasks_failed_on_worker_dead(
                    worker_table_data.worker_address.worker_id, worker_table_data
                )

    async def _reconcile_finished_jobs_on_startup(self) -> None:
        # GCS pubsub does not replay job finishes from before we subscribed, so snapshot
        # the job table once and fail tasks for any already-finished job. This must run
        # AFTER subscribe() so a finish between the snapshot and the subscription is still
        # delivered over pubsub.
        try:
            # Overlap the snapshot fetch with the same delay _on_job_finished uses, so
            # in-flight FINISHED events can land before we fail the tasks.
            job_infos, _ = await asyncio.gather(
                self._get_all_job_info(),
                asyncio.sleep(_MARK_FAILED_ON_JOB_DONE_DELAY_S),
            )
        except Exception:
            logger.exception("Failed to reconcile finished jobs on startup.")
            return
        for job_data in job_infos:
            if job_data.is_dead:
                self._store.mark_tasks_failed_on_job_ends(
                    job_data.job_id, job_data.end_time * 10**6
                )
                self._store.update_job_summary_on_job_done(job_data.job_id)

    async def _subscribe_for_worker_deaths(self) -> None:
        subscriber = GcsAioWorkerDeltaSubscriber(address=self._gcs_address)
        await subscriber.subscribe()
        # Backfill deaths from before the subscription (pubsub won't replay them). Order
        # matters: subscribe first, then snapshot, so nothing falls between the two.
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
        subscriber = GcsAioJobSubscriber(address=self._gcs_address)
        await subscriber.subscribe()
        # Backfill finishes from before the subscription (pubsub won't replay them).
        # Spawn it (unlike the worker path) so the 15s job-done delay doesn't stall the
        # poll loop; a finish also seen via pubsub is fine since marking is idempotent.
        self._spawn(self._reconcile_finished_jobs_on_startup())
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
