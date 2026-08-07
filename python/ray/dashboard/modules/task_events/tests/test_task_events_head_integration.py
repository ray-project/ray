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
from ray.dashboard.modules.task_events.task_events_head import TaskEventsHead
from ray.dashboard.subprocesses.module import SubprocessModuleConfig
from ray.tests.conftest import *  # noqa


def _make_head(gcs_address: str) -> TaskEventsHead:
    """Build a TaskEventsHead directly in the test's driver process (not as its usual
    separate SubprocessModule) so the test can drive its subscription loops and read its
    in-memory buffers in-process. Test-only."""
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
    return TaskEventsHead(config)


async def _stop(subscription: asyncio.Task) -> None:
    subscription.cancel()
    try:
        await subscription
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_worker_death_lands_in_buffer(ray_start_regular):
    """A killed worker's failure is delivered over GCS pubsub and buffered by the head."""
    gcs_address = ray_start_regular["gcs_address"]
    head = _make_head(gcs_address)

    subscription = asyncio.create_task(head._subscribe_for_worker_deaths())
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

        await async_wait_for_condition(
            lambda: worker_id in {wd.worker_id for wd in head._dead_workers},
            timeout=30,
        )
    finally:
        await _stop(subscription)


@pytest.mark.asyncio
async def test_job_finished_lands_in_buffer(ray_start_regular):
    """A finished job is delivered over GCS pubsub and buffered by the head."""
    gcs_address = ray_start_regular["gcs_address"]
    head = _make_head(gcs_address)

    subscription = asyncio.create_task(head._subscribe_for_finished_jobs())
    try:
        await asyncio.sleep(2)

        # A driver that connects and immediately exits, finishing its job. Run it off the
        # event loop so the subscription keeps polling while the subprocess runs.
        driver = f'import ray\nray.init(address="{gcs_address}")\n'
        await asyncio.get_running_loop().run_in_executor(
            None, run_string_as_driver, driver
        )

        await async_wait_for_condition(
            lambda: any(job.is_dead for job in head._finished_jobs),
            timeout=30,
        )
    finally:
        await _stop(subscription)


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
