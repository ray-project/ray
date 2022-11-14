import os
import time
import mmap
import json
import fnmatch
import asyncio
import itertools
import collections
import logging.handlers
from ray._private.utils import get_or_create_event_loop

from concurrent.futures import ThreadPoolExecutor

from ray.dashboard.modules.event import event_consts
from ray.dashboard.utils import async_loop_forever, create_task

logger = logging.getLogger(__name__)


def _get_source_files(event_dir, source_types=None, event_file_filter=None):
    event_log_names = os.listdir(event_dir)
    source_files = {}
    all_source_types = set(event_consts.EVENT_SOURCE_ALL)
    for source_type in source_types or event_consts.EVENT_SOURCE_ALL:
        assert source_type in all_source_types, f"Invalid source type: {source_type}"
        files = []
        for n in event_log_names:
            if fnmatch.fnmatch(n, f"*{source_type}*"):
                f = os.path.join(event_dir, n)
                if event_file_filter is not None and not event_file_filter(f):
                    continue
                files.append(f)
        if files:
            source_files[source_type] = files
    return source_files


def _restore_newline(event_dict):
    try:
        event_dict["message"] = (
            event_dict["message"].replace("\\n", "\n").replace("\\r", "\n")
        )
    except Exception:
        logger.exception("Restore newline for event failed: %s", event_dict)
    return event_dict


def _parse_line(event_str):
    return _restore_newline(json.loads(event_str))


def parse_event_strings(event_string_list):
    events = []
    for data in event_string_list:
        if not data:
            continue
        try:
            event = _parse_line(data)
            events.append(event)
        except Exception:
            logger.exception("Parse event line failed: %s", repr(data))
    return events


ReadFileResult = collections.namedtuple(
    "ReadFileResult", ["fid", "size", "mtime", "position", "lines"]
)


def _read_file(
    file, pos, n_lines=event_consts.EVENT_READ_LINE_COUNT_LIMIT, closefd=True
):
    with open(file, "rb", closefd=closefd) as f:
        # The ino may be 0 on Windows.
        stat = os.stat(f.fileno())
        fid = stat.st_ino or file
        lines = []
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            start = pos
            for _ in range(n_lines):
                sep = mm.find(b"\n", start)
                if sep == -1:
                    break
                if sep - start <= event_consts.EVENT_READ_LINE_LENGTH_LIMIT:
                    lines.append(mm[start:sep].decode("utf-8"))
                else:
                    truncated_size = min(100, event_consts.EVENT_READ_LINE_LENGTH_LIMIT)
                    logger.warning(
                        "Ignored long string: %s...(%s chars)",
                        mm[start : start + truncated_size].decode("utf-8"),
                        sep - start,
                    )
                start = sep + 1
        return ReadFileResult(fid, stat.st_size, stat.st_mtime, start, lines)


def monitor_events(
    event_dir,
    callback,
    monitor_thread_pool_executor: ThreadPoolExecutor,
    scan_interval_seconds=event_consts.SCAN_EVENT_DIR_INTERVAL_SECONDS,
    start_mtime=time.time() + event_consts.SCAN_EVENT_START_OFFSET_SECONDS,
    monitor_files=None,
    source_types=None,
):
    """Monitor events in directory. New events will be read and passed to the
    callback.

    Args:
        event_dir: The event log directory.
        callback (def callback(List[str]): pass): A callback accepts a list of
            event strings.
        monitor_thread_pool_executor: A thread pool exector to monitor/update
            events. None means it will use the default execturo which uses
            num_cpus of the machine * 5 threads (before python 3.8) or
            min(32, num_cpus + 5) (from Python 3.8).
        scan_interval_seconds: An interval seconds between two scans.
        start_mtime: Only the event log files whose last modification
            time is greater than start_mtime are monitored.
        monitor_files (Dict[int, MonitorFile]): The map from event log file id
            to MonitorFile object. Monitor all files start from the beginning
            if the value is None.
        source_types (List[str]): A list of source type name from
            event_pb2.Event.SourceType.keys(). Monitor all source types if the
            value is None.
    """
    loop = get_or_create_event_loop()
    if monitor_files is None:
        monitor_files = {}

    logger.info(
        "Monitor events logs modified after %s on %s, " "the source types are %s.",
        start_mtime,
        event_dir,
        "all" if source_types is None else source_types,
    )

    MonitorFile = collections.namedtuple("MonitorFile", ["size", "mtime", "position"])

    def _source_file_filter(source_file):
        stat = os.stat(source_file)
        return stat.st_mtime > start_mtime

    def _read_monitor_file(file, pos):
        assert isinstance(
            file, str
        ), f"File should be a str, but a {type(file)}({file}) found"
        fd = os.open(file, os.O_RDONLY)
        try:
            stat = os.stat(fd)
            # Check the file size to avoid raising the exception
            # ValueError: cannot mmap an empty file
            if stat.st_size <= 0:
                return []
            fid = stat.st_ino or file
            monitor_file = monitor_files.get(fid)
            if monitor_file:
                if (
                    monitor_file.position == monitor_file.size
                    and monitor_file.size == stat.st_size
                    and monitor_file.mtime == stat.st_mtime
                ):
                    logger.debug(
                        "Skip reading the file because " "there is no change: %s", file
                    )
                    return []
                position = monitor_file.position
            else:
                logger.info("Found new event log file: %s", file)
                position = pos
            # Close the fd in finally.
            r = _read_file(fd, position, closefd=False)
            # It should be fine to update the dict in executor thread.
            monitor_files[r.fid] = MonitorFile(r.size, r.mtime, r.position)
            loop.call_soon_threadsafe(callback, r.lines)
        except Exception as e:
            raise Exception(f"Read event file failed: {file}") from e
        finally:
            os.close(fd)

    @async_loop_forever(scan_interval_seconds, cancellable=True)
    async def _scan_event_log_files():
        # Scan event files.
        source_files = await loop.run_in_executor(
            monitor_thread_pool_executor,
            _get_source_files,
            event_dir,
            source_types,
            _source_file_filter,
        )

        # Limit concurrent read to avoid fd exhaustion.
        semaphore = asyncio.Semaphore(event_consts.CONCURRENT_READ_LIMIT)

        async def _concurrent_coro(filename):
            async with semaphore:
                return await loop.run_in_executor(
                    monitor_thread_pool_executor, _read_monitor_file, filename, 0
                )

        # Read files.
        await asyncio.gather(
            *[
                _concurrent_coro(filename)
                for filename in list(itertools.chain(*source_files.values()))
            ]
        )

    return create_task(_scan_event_log_files())
