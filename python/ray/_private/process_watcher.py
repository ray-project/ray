import asyncio
import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor

import ray
import ray._private.ray_constants as ray_constants
import ray.dashboard.consts as dashboard_consts
from ray._common.utils import run_background_task
from ray._raylet import GcsClient
from ray.dashboard.consts import _PARENT_DEATH_THREASHOLD

# Import psutil after ray so the packaged version is used.
import psutil

logger = logging.getLogger(__name__)

# TODO: move all consts from dashboard_consts to ray_constants and rename to remove
# DASHBOARD_ prefixes.

# Publishes at most this number of lines of Raylet logs, when the Raylet dies
# unexpectedly.
_RAYLET_LOG_MAX_PUBLISH_LINES = 20

# Reads at most this amount of Raylet logs from the tail, for publishing and
# checking if the Raylet was terminated gracefully.
_RAYLET_LOG_MAX_TAIL_SIZE = 1 * 1024**2

try:
    create_task = asyncio.create_task
except AttributeError:
    create_task = asyncio.ensure_future


def get_raylet_pid():
    # TODO(edoakes): RAY_RAYLET_PID isn't properly set on Windows. This is
    # only used for fate-sharing with the raylet and we need a different
    # fate-sharing mechanism for Windows anyways.
    if sys.platform in ["win32", "cygwin"]:
        return None
    raylet_pid = int(os.environ["RAY_RAYLET_PID"])
    assert raylet_pid > 0
    logger.info("raylet pid is %s", raylet_pid)
    return raylet_pid


def create_check_raylet_task(log_dir, gcs_client, parent_dead_callback, loop):
    """
    Creates an asyncio task to periodically check if the raylet process is still
    running. If raylet is dead for _PARENT_DEATH_THREASHOLD (5) times, prepare to exit
    as follows:

    - Write logs about whether the raylet exit is graceful, by looking into the raylet
    log and search for term "SIGTERM",
    - Flush the logs via GcsClient,
    - Exit.
    """
    if sys.platform in ["win32", "cygwin"]:
        raise RuntimeError("can't check raylet process in Windows.")
    raylet_pid = get_raylet_pid()

    if dashboard_consts.PARENT_HEALTH_CHECK_BY_PIPE:
        logger.info("check_parent_via_pipe")
        check_parent_task = _check_parent_via_pipe(
            log_dir, gcs_client, loop, parent_dead_callback
        )
    else:
        logger.info("_check_parent")
        check_parent_task = _check_parent(
            raylet_pid, log_dir, gcs_client, parent_dead_callback
        )

    return run_background_task(check_parent_task)


def report_raylet_error_logs(log_dir: str, gcs_client: GcsClient):
    log_path = os.path.join(log_dir, "raylet.out")
    error = False
    msg = "Raylet is terminated. "
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            # Seek to _RAYLET_LOG_MAX_TAIL_SIZE from the end if the
            # file is larger than that.
            f.seek(0, io.SEEK_END)
            pos = max(0, f.tell() - _RAYLET_LOG_MAX_TAIL_SIZE)
            f.seek(pos, io.SEEK_SET)
            # Read remaining logs by lines.
            raylet_logs = f.readlines()
            # Assume the SIGTERM message must exist within the last
            # _RAYLET_LOG_MAX_TAIL_SIZE of the log file.
            if any("Raylet received SIGTERM" in line for line in raylet_logs):
                msg += "Termination is graceful."
                logger.info(msg)
            else:
                msg += (
                    "Termination is unexpected. Possible reasons "
                    "include: (1) SIGKILL by the user or system "
                    "OOM killer, (2) Invalid memory access from "
                    "Raylet causing SIGSEGV or SIGBUS, "
                    "(3) Other termination signals. "
                    f"Last {_RAYLET_LOG_MAX_PUBLISH_LINES} lines "
                    "of the Raylet logs:\n"
                )
                msg += "    " + "    ".join(
                    raylet_logs[-_RAYLET_LOG_MAX_PUBLISH_LINES:]
                )
                error = True
    except Exception as e:
        msg += f"Failed to read Raylet logs at {log_path}: {e}!"
        logger.exception(msg)
        error = True
    if error:
        logger.error(msg)
        # TODO: switch to async if necessary.
        ray._private.utils.publish_error_to_driver(
            ray_constants.RAYLET_DIED_ERROR,
            msg,
            gcs_client=gcs_client,
        )
    else:
        logger.info(msg)


async def _check_parent_via_pipe(
    log_dir: str, gcs_client: GcsClient, loop, parent_dead_callback
):
    while True:
        try:
            # Read input asynchronously.
            # The parent (raylet) should have redirected its pipe
            # to stdin. If we read 0 bytes from stdin, it means
            # the process is dead.
            with ThreadPoolExecutor(max_workers=1) as executor:
                input_data = await loop.run_in_executor(
                    executor, lambda: sys.stdin.readline()
                )
            if len(input_data) == 0:
                # cannot read bytes from parent == parent is dead.
                parent_dead_callback("_check_parent_via_pipe: The parent is dead.")
                report_raylet_error_logs(log_dir, gcs_client)
                sys.exit(0)
        except Exception as e:
            logger.exception(
                "raylet health checking is failed. "
                f"The agent process may leak. Exception: {e}"
            )


async def _check_parent(raylet_pid, log_dir, gcs_client, parent_dead_callback):
    """Check if raylet is dead and fate-share if it is."""
    try:
        curr_proc = psutil.Process()
        parent_death_cnt = 0
        while True:
            parent = curr_proc.parent()
            # If the parent is dead, it is None.
            parent_gone = parent is None
            init_assigned_for_parent = False
            parent_changed = False

            if parent:
                # Sometimes, the parent is changed to the `init` process.
                # In this case, the parent.pid is 1.
                init_assigned_for_parent = parent.pid == 1
                # Sometimes, the parent is dead, and the pid is reused
                # by other processes. In this case, this condition is triggered.
                parent_changed = raylet_pid != parent.pid

            if parent_gone or init_assigned_for_parent or parent_changed:
                parent_death_cnt += 1
                logger.warning(
                    f"Raylet is considered dead {parent_death_cnt} X. "
                    f"If it reaches to {_PARENT_DEATH_THREASHOLD}, the agent "
                    f"will kill itself. Parent: {parent}, "
                    f"parent_gone: {parent_gone}, "
                    f"init_assigned_for_parent: {init_assigned_for_parent}, "
                    f"parent_changed: {parent_changed}."
                )
                if parent_death_cnt < _PARENT_DEATH_THREASHOLD:
                    await asyncio.sleep(
                        dashboard_consts.DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S
                    )
                    continue

                parent_dead_callback("_check_parent: The parent is dead.")
                report_raylet_error_logs(log_dir, gcs_client)
                sys.exit(0)
            else:
                parent_death_cnt = 0
            await asyncio.sleep(
                dashboard_consts.DASHBOARD_AGENT_CHECK_PARENT_INTERVAL_S
            )
    except Exception:
        logger.exception("Failed to check parent PID, exiting.")
        sys.exit(1)
