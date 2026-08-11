import asyncio
import sys

import pytest

import ray
import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.ray_constants import (
    LOGGING_ROTATE_BACKUP_COUNT,
    LOGGING_ROTATE_BYTES,
)
from ray._common.test_utils import async_wait_for_condition, run_string_as_driver
from ray.dashboard.modules.task_events.task_event_manager import TaskEventManager
from ray.dashboard.modules.task_events.task_events_head import TaskEventsHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.tests.conftest import *  # noqa

_MANAGER = "ray.dashboard.modules.task_events.task_event_manager"


def _make_manager(gcs_address: str) -> TaskEventManager:
    """Build a TaskEventManager directly in the test's driver process (not inside its usual
    separate SubprocessModule) so the test can drive its subscription loops and inspect its
    in-memory store in-process. Reuses TaskEventsHead only to obtain a GCS aio channel wired
    to the test cluster. Test-only."""
    config = SubprocessModuleConfig(
        cluster_id_hex="deadbeef",
        gcs_address=gcs_address,
        session_name="test_session",
        temp_dir="/tmp",
        session_dir="/tmp",
        logging_level=ray_constants.LOGGER_LEVEL,
        logging_format=ray_constants.LOGGER_FORMAT,
        log_dir="/tmp",
        logging_filename=dashboard_consts.DASHBOARD_LOG_FILENAME,
        logging_rotate_bytes=LOGGING_ROTATE_BYTES,
        logging_rotate_backup_count=LOGGING_ROTATE_BACKUP_COUNT,
        socket_dir="/tmp",
    )
    head = TaskEventsHead(config)
    return TaskEventManager(head._store, head.gcs_address, head.aiogrpc_gcs_channel)


async def _stop(subscription: asyncio.Task) -> None:
    subscription.cancel()
    try:
        await subscription
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_worker_death_triggers_reconciliation(ray_start_regular, monkeypatch):
    """A killed worker's failure is delivered over GCS pubsub, its exit info is fetched,
    and the manager fails that worker's tasks."""
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_WORKER_DEAD_DELAY_S", 0.0)
    gcs_address = ray_start_regular["gcs_address"]
    manager = _make_manager(gcs_address)

    marked_workers = []
    original = manager._store.mark_tasks_failed_on_worker_dead

    def record(worker_id, worker_table_data):
        marked_workers.append(worker_id)
        return original(worker_id, worker_table_data)

    monkeypatch.setattr(manager._store, "mark_tasks_failed_on_worker_dead", record)

    subscription = asyncio.create_task(manager._subscribe_for_worker_deaths())
    try:
        # Let the subscription register before triggering the death: GCS only queues
        # messages for a subscriber once its subscription is established.
        await asyncio.sleep(2)

        @ray.remote
        class Actor:
            def get_worker_id(self):
                return ray.get_runtime_context().get_worker_id()

        actor = Actor.remote()
        worker_id = bytes.fromhex(await actor.get_worker_id.remote())
        ray.kill(actor, no_restart=True)

        # Reaching the store call means the worker delta arrived and GetWorkerInfo
        # returned the dead worker's exit info (otherwise reconciliation returns early).
        await async_wait_for_condition(lambda: worker_id in marked_workers, timeout=30)
    finally:
        await _stop(subscription)


@pytest.mark.asyncio
async def test_job_finished_triggers_reconciliation(ray_start_regular, monkeypatch):
    """A finished job is delivered over GCS pubsub and the manager fails that job's
    tasks. Running-job updates are ignored, so only the finished job is acted on."""
    monkeypatch.setattr(f"{_MANAGER}._MARK_FAILED_ON_JOB_DONE_DELAY_S", 0.0)
    gcs_address = ray_start_regular["gcs_address"]
    manager = _make_manager(gcs_address)

    marked_jobs = []
    original = manager._store.mark_tasks_failed_on_job_ends

    def record(job_id, job_finish_time_ns):
        marked_jobs.append(job_id)
        return original(job_id, job_finish_time_ns)

    monkeypatch.setattr(manager._store, "mark_tasks_failed_on_job_ends", record)

    subscription = asyncio.create_task(manager._subscribe_for_finished_jobs())
    try:
        await asyncio.sleep(2)

        # A driver that connects, reports its job id, and exits (finishing its job). Run
        # it off the event loop so the subscription keeps polling while it runs.
        driver = (
            "import ray\n"
            f'ray.init(address="{gcs_address}")\n'
            'print("JOBID:" + ray.get_runtime_context().get_job_id())\n'
        )
        output = await asyncio.get_running_loop().run_in_executor(
            None, run_string_as_driver, driver
        )
        job_line = next(
            line for line in output.splitlines() if line.startswith("JOBID:")
        )
        job_id = bytes.fromhex(job_line[len("JOBID:") :].strip())

        await async_wait_for_condition(lambda: job_id in marked_jobs, timeout=30)
    finally:
        await _stop(subscription)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
