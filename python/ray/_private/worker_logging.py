import colorama
import logging
import os
import sys
import time
from ray.experimental import tqdm_ray
from ray.experimental.tqdm_ray import RAY_TQDM_MAGIC
from ray.util.debug import log_once
import json
from typing import (
    Any,
    Dict,
    List,
)
import ray._private.ray_constants as ray_constants

# Ray modules
import ray.actor
import ray.job_config
from ray._private.ray_logging import (
    stdout_deduplicator,
    stderr_deduplicator,
)

"""
Frontend printing functions. Performs log deduplication, coloring and printing.
May be run in "standalone" mode where Ray is not inited.
"""

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


# When we enter a breakpoint, worker logs are automatically disabled via this.
_worker_logs_enabled = True


# Start time of this process, used for relative time logs.
t0 = time.time()
autoscaler_log_fyi_printed = False


def process_tqdm(line):
    """Experimental distributed tqdm: see ray.experimental.tqdm_ray."""
    try:
        data = json.loads(line)
        tqdm_ray.instance().process_state_update(data)
    except Exception:
        if log_once("tqdm_corruption"):
            logger.warning(
                f"[tqdm_ray] Failed to decode {line}, this may be due to "
                "logging too fast. This warning will not be printed again."
            )


def hide_tqdm():
    """Hide distributed tqdm bars temporarily to avoid conflicts with other logs."""
    tqdm_ray.instance().hide_bars()


def restore_tqdm():
    """Undo hide_tqdm()."""
    tqdm_ray.instance().unhide_bars()


def filter_autoscaler_events(lines: List[str]) -> Iterator[str]:
    """Given raw log lines from the monitor, return only autoscaler events.

    For Autoscaler V1:
        Autoscaler events are denoted by the ":event_summary:" magic token.
    For Autoscaler V2:
        Autoscaler events are published from log_monitor.py which read
        them from the `event_AUTOSCALER.log`.
    """

    if not ray_constants.AUTOSCALER_EVENTS:
        return

    AUTOSCALER_LOG_FYI = (
        "Tip: use `ray status` to view detailed "
        "cluster status. To disable these "
        "messages, set RAY_SCHEDULER_EVENTS=0."
    )

    def autoscaler_log_fyi_needed() -> bool:
        global autoscaler_log_fyi_printed
        if not autoscaler_log_fyi_printed:
            autoscaler_log_fyi_printed = True
            return True
        return False

    from ray.autoscaler.v2.utils import is_autoscaler_v2

    if is_autoscaler_v2():
        from ray._private.event.event_logger import parse_event, filter_event_by_level

        for event_line in lines:
            if autoscaler_log_fyi_needed():
                yield AUTOSCALER_LOG_FYI

            event = parse_event(event_line)
            if not event or not event.message:
                continue

            if filter_event_by_level(
                event, ray_constants.RAY_LOG_TO_DRIVER_EVENT_LEVEL
            ):
                continue

            yield event.message
    else:
        # Print out autoscaler events only, ignoring other messages.
        for line in lines:
            if ray_constants.LOG_PREFIX_EVENT_SUMMARY in line:
                if autoscaler_log_fyi_needed():
                    yield AUTOSCALER_LOG_FYI
                # The event text immediately follows the ":event_summary:"
                # magic token.
                yield line.split(ray_constants.LOG_PREFIX_EVENT_SUMMARY)[1]


def time_string() -> str:
    """Return the relative time from the start of this job.

    For example, 15m30s.
    """
    delta = time.time() - t0
    hours = 0
    minutes = 0
    while delta > 3600:
        hours += 1
        delta -= 3600
    while delta > 60:
        minutes += 1
        delta -= 60
    output = ""
    if hours:
        output += f"{hours}h"
    if minutes:
        output += f"{minutes}m"
    output += f"{int(delta)}s"
    return output


def print_worker_logs(data: Dict[str, str], print_file: Any):
    if not _worker_logs_enabled:
        return

    def prefix_for(data: Dict[str, str]) -> str:
        """The PID prefix for this log line."""
        if data.get("pid") in ["autoscaler", "raylet"]:
            return ""
        else:
            res = "pid="
            if data.get("actor_name"):
                res = f"{data['actor_name']} {res}"
            elif data.get("task_name"):
                res = f"{data['task_name']} {res}"
            return res

    def message_for(data: Dict[str, str], line: str) -> str:
        """The printed message of this log line."""
        if ray_constants.LOG_PREFIX_INFO_MESSAGE in line:
            return line.split(ray_constants.LOG_PREFIX_INFO_MESSAGE)[1]
        return line

    def color_for(data: Dict[str, str], line: str) -> str:
        """The color for this log line."""
        if (
            data.get("pid") == "raylet"
            and ray_constants.LOG_PREFIX_INFO_MESSAGE not in line
        ):
            return colorama.Fore.YELLOW
        elif data.get("pid") == "autoscaler":
            if "Error:" in line or "Warning:" in line:
                return colorama.Fore.YELLOW
            else:
                return colorama.Fore.CYAN
        elif os.getenv("RAY_COLOR_PREFIX") == "1":
            colors = [
                # colorama.Fore.BLUE, # Too dark
                colorama.Fore.MAGENTA,
                colorama.Fore.CYAN,
                colorama.Fore.GREEN,
                # colorama.Fore.WHITE, # Too light
                # colorama.Fore.RED,
                colorama.Fore.LIGHTBLACK_EX,
                colorama.Fore.LIGHTBLUE_EX,
                # colorama.Fore.LIGHTCYAN_EX, # Too light
                # colorama.Fore.LIGHTGREEN_EX, # Too light
                colorama.Fore.LIGHTMAGENTA_EX,
                # colorama.Fore.LIGHTWHITE_EX, # Too light
                # colorama.Fore.LIGHTYELLOW_EX, # Too light
            ]
            pid = data.get("pid", 0)
            try:
                i = int(pid)
            except ValueError:
                i = 0
            return colors[i % len(colors)]
        else:
            return colorama.Fore.CYAN

    if data.get("pid") == "autoscaler":
        pid = "autoscaler +{}".format(time_string())
        lines = filter_autoscaler_events(data.get("lines", []))
    else:
        pid = data.get("pid")
        lines = data.get("lines", [])

    if data.get("ip") == data.get("localhost"):
        for line in lines:
            if RAY_TQDM_MAGIC in line:
                process_tqdm(line)
            else:
                hide_tqdm()
                print(
                    "{}({}{}){} {}".format(
                        color_for(data, line),
                        prefix_for(data),
                        pid,
                        colorama.Style.RESET_ALL,
                        message_for(data, line),
                    ),
                    file=print_file,
                )
    else:
        for line in lines:
            if RAY_TQDM_MAGIC in line:
                process_tqdm(line)
            else:
                hide_tqdm()
                print(
                    "{}({}{}, ip={}){} {}".format(
                        color_for(data, line),
                        prefix_for(data),
                        pid,
                        data.get("ip"),
                        colorama.Style.RESET_ALL,
                        message_for(data, line),
                    ),
                    file=print_file,
                )
    # Restore once at end of batch to avoid excess hiding/unhiding of tqdm.
    restore_tqdm()


def print_to_stdstream(data):
    should_dedup = data.get("pid") not in ["autoscaler"]

    if data["is_err"]:
        if should_dedup:
            batches = stderr_deduplicator.deduplicate(data)
        else:
            batches = [data]
        sink = sys.stderr
    else:
        if should_dedup:
            batches = stdout_deduplicator.deduplicate(data)
        else:
            batches = [data]
        sink = sys.stdout

    for batch in batches:
        print_worker_logs(batch, sink)
