import logging
import logging.handlers
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import colorama

import ray
from ray._private.ray_constants import (
    RAY_DEDUP_LOGS,
    RAY_DEDUP_LOGS_AGG_WINDOW_S,
    RAY_DEDUP_LOGS_ALLOW_REGEX,
    RAY_DEDUP_LOGS_SKIP_REGEX,
)
from ray.experimental.tqdm_ray import RAY_TQDM_MAGIC
from ray.util.debug import log_once


def setup_logger(
    logging_level: int,
    logging_format: str,
):
    """Setup default logging for ray."""
    logger = logging.getLogger("ray")
    if logging_format:
        # Overwrite the formatters for all default handlers.
        formatter = logging.Formatter(logging_format)
        for handler in logger.handlers:
            handler.setFormatter(formatter)
    if type(logging_level) is str:
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)


def setup_component_logger(
    *,
    logging_level,
    logging_format,
    log_dir,
    filename: Union[str, Iterable[str]],
    max_bytes,
    backup_count,
    logger_name=None,
    propagate=True,
):
    """Configure the logger that is used for Ray's python components.

    For example, it should be used for monitor, dashboard, and log monitor.
    The only exception is workers. They use the different logging config.

    Ray's python components generally should not write to stdout/stderr, because
    messages written there will be redirected to the head node. For deployments where
    there may be thousands of workers, this would create unacceptable levels of log
    spam. For this reason, we disable the "ray" logger's handlers, and enable
    propagation so that log messages that actually do need to be sent to the head node
    can reach it.

    Args:
        logging_level: Logging level in string or logging enum.
        logging_format: Logging format string.
        log_dir: Log directory path. If empty, logs will go to
            stderr.
        filename: A single filename or an iterable of filenames to write logs to.
            If empty, logs will go to stderr.
        max_bytes: Same argument as RotatingFileHandler's maxBytes.
        backup_count: Same argument as RotatingFileHandler's backupCount.
        logger_name: Used to create or get the correspoding
            logger in getLogger call. It will get the root logger by default.
        propagate: Whether to propagate the log to the parent logger.
    Returns:
        the created or modified logger.
    """
    ray._private.log.clear_logger("ray")

    logger = logging.getLogger(logger_name)
    if isinstance(logging_level, str):
        logging_level = logging.getLevelName(logging_level.upper())
    logger.setLevel(logging_level)

    filenames = [filename] if isinstance(filename, str) else filename

    for filename in filenames:
        if not filename or not log_dir:
            handler = logging.StreamHandler()
        else:
            handler = logging.handlers.RotatingFileHandler(
                os.path.join(log_dir, filename),
                maxBytes=max_bytes,
                backupCount=backup_count,
            )
        handler.setLevel(logging_level)
        handler.setFormatter(logging.Formatter(logging_format))
        logger.addHandler(handler)

    logger.propagate = propagate
    return logger


def run_callback_on_events_in_ipython(event: str, cb: Callable):
    """
    Register a callback to be run after each cell completes in IPython.
    E.g.:
        This is used to flush the logs after each cell completes.

    If IPython is not installed, this function does nothing.

    Args:
        cb: The callback to run.
    """
    if "IPython" in sys.modules:
        from IPython import get_ipython

        ipython = get_ipython()
        # Register a callback on cell completion.
        if ipython is not None:
            ipython.events.register(event, cb)


"""
All components underneath here is used specifically for the default_worker.py.
"""

# It's worth noticing that filepath format should be kept in sync with function
# `GetWorkerOutputFilepath` under file "src/ray/core_worker/core_worker_process.cc".
def get_worker_log_file_name(worker_type, job_id=None):
    if job_id is None:
        job_id = os.environ.get("RAY_JOB_ID")
    if worker_type == "WORKER":
        if job_id is None:
            job_id = ""
        worker_name = "worker"
    else:
        job_id = ""
        worker_name = "io_worker"

    # Make sure these values are set already.
    assert ray._private.worker._global_node is not None
    assert ray._private.worker.global_worker is not None
    filename = f"{worker_name}-{ray.get_runtime_context().get_worker_id()}-"
    if job_id:
        filename += f"{job_id}-"
    filename += f"{os.getpid()}"
    return filename


def configure_log_file(out_file, err_file):
    # If either of the file handles are None, there are no log files to
    # configure since we're redirecting all output to stdout and stderr.
    if out_file is None or err_file is None:
        return
    stdout_fileno = sys.stdout.fileno()
    stderr_fileno = sys.stderr.fileno()
    # C++ logging requires redirecting the stdout file descriptor. Note that
    # dup2 will automatically close the old file descriptor before overriding
    # it.
    os.dup2(out_file.fileno(), stdout_fileno)
    os.dup2(err_file.fileno(), stderr_fileno)
    # We also manually set sys.stdout and sys.stderr because that seems to
    # have an effect on the output buffering. Without doing this, stdout
    # and stderr are heavily buffered resulting in seemingly lost logging
    # statements. We never want to close the stdout file descriptor, dup2 will
    # close it when necessary and we don't want python's GC to close it.
    sys.stdout = ray._private.utils.open_log(
        stdout_fileno, unbuffered=True, closefd=False
    )
    sys.stderr = ray._private.utils.open_log(
        stderr_fileno, unbuffered=True, closefd=False
    )


class WorkerStandardStreamDispatcher:
    def __init__(self):
        self.handlers = []
        self._lock = threading.Lock()

    def add_handler(self, name: str, handler: Callable) -> None:
        with self._lock:
            self.handlers.append((name, handler))

    def remove_handler(self, name: str) -> None:
        with self._lock:
            new_handlers = [pair for pair in self.handlers if pair[0] != name]
            self.handlers = new_handlers

    def emit(self, data):
        with self._lock:
            for pair in self.handlers:
                _, handle = pair
                handle(data)


global_worker_stdstream_dispatcher = WorkerStandardStreamDispatcher()


# Regex for canonicalizing log lines.
NUMBERS = re.compile(r"(\d+|0x[0-9a-fA-F]+)")

# Batch of log lines including ip, pid, lines, etc.
LogBatch = Dict[str, Any]


def _canonicalise_log_line(line):
    # Remove words containing numbers or hex, since those tend to differ between
    # workers.
    return " ".join(x for x in line.split() if not NUMBERS.search(x))


@dataclass
class DedupState:
    # Timestamp of the earliest log message seen of this pattern.
    timestamp: int

    # The number of un-printed occurrances for this pattern.
    count: int

    # Latest instance of this log pattern.
    line: int

    # Latest metadata dict for this log pattern, not including the lines field.
    metadata: LogBatch

    # Set of (ip, pid) sources which have emitted this pattern.
    sources: Set[Tuple[str, int]]

    # The string that should be printed to stdout.
    def formatted(self) -> str:
        return self.line + _color(
            f" [repeated {self.count}x across cluster]" + _warn_once()
        )


class LogDeduplicator:
    def __init__(
        self,
        agg_window_s: int,
        allow_re: Optional[str],
        skip_re: Optional[str],
        *,
        _timesource=None,
    ):
        self.agg_window_s = agg_window_s
        if allow_re:
            self.allow_re = re.compile(allow_re)
        else:
            self.allow_re = None
        if skip_re:
            self.skip_re = re.compile(skip_re)
        else:
            self.skip_re = None
        # Buffer of up to RAY_DEDUP_LOGS_AGG_WINDOW_S recent log patterns.
        # This buffer is cleared if the pattern isn't seen within the window.
        self.recent: Dict[str, DedupState] = {}
        self.timesource = _timesource or (lambda: time.time())

        run_callback_on_events_in_ipython("post_execute", self.flush)

    def deduplicate(self, batch: LogBatch) -> List[LogBatch]:
        """Rewrite a batch of lines to reduce duplicate log messages.

        Args:
            batch: The batch of lines from a single source.

        Returns:
            List of batches from this and possibly other previous sources to print.
        """
        if not RAY_DEDUP_LOGS:
            return [batch]

        now = self.timesource()
        metadata = batch.copy()
        del metadata["lines"]
        source = (metadata.get("ip"), metadata.get("pid"))
        output: List[LogBatch] = [dict(**metadata, lines=[])]

        # Decide which lines to emit from the input batch. Put the outputs in the
        # first output log batch (output[0]).
        for line in batch["lines"]:
            if RAY_TQDM_MAGIC in line or (self.allow_re and self.allow_re.search(line)):
                output[0]["lines"].append(line)
                continue
            elif self.skip_re and self.skip_re.search(line):
                continue
            dedup_key = _canonicalise_log_line(line)

            if dedup_key == "":
                # Don't dedup messages that are empty after canonicalization.
                # Because that's all the information users want to see.
                output[0]["lines"].append(line)
                continue

            if dedup_key in self.recent:
                sources = self.recent[dedup_key].sources
                sources.add(source)
                # We deduplicate the warnings/error messages from raylet by default.
                if len(sources) > 1 or batch["pid"] == "raylet":
                    state = self.recent[dedup_key]
                    self.recent[dedup_key] = DedupState(
                        state.timestamp,
                        state.count + 1,
                        line,
                        metadata,
                        sources,
                    )
                else:
                    # Don't dedup messages from the same source, just print.
                    output[0]["lines"].append(line)
            else:
                self.recent[dedup_key] = DedupState(now, 0, line, metadata, {source})
                output[0]["lines"].append(line)

        # Flush patterns from the buffer that are older than the aggregation window.
        while self.recent:
            if now - next(iter(self.recent.values())).timestamp < self.agg_window_s:
                break
            dedup_key = next(iter(self.recent))
            state = self.recent.pop(dedup_key)
            # we already logged an instance of this line immediately when received,
            # so don't log for count == 0
            if state.count > 1:
                # (Actor pid=xxxx) [repeated 2x across cluster] ...
                output.append(dict(**state.metadata, lines=[state.formatted()]))
                # Continue aggregating for this key but reset timestamp and count.
                state.timestamp = now
                state.count = 0
                self.recent[dedup_key] = state
            elif state.count > 0:
                # Aggregation wasn't fruitful, print the line and stop aggregating.
                output.append(dict(state.metadata, lines=[state.line]))

        return output

    def flush(self) -> List[dict]:
        """Return all buffered log messages and clear the buffer.

        Returns:
            List of log batches to print.
        """
        output = []
        for state in self.recent.values():
            if state.count > 1:
                output.append(
                    dict(
                        state.metadata,
                        lines=[state.formatted()],
                    )
                )
            elif state.count > 0:
                output.append(dict(state.metadata, **{"lines": [state.line]}))
        self.recent.clear()
        return output


def _warn_once() -> str:
    if log_once("log_dedup_warning"):
        return (
            " (Ray deduplicates logs by default. Set RAY_DEDUP_LOGS=0 to "
            "disable log deduplication, or see https://docs.ray.io/en/master/"
            "ray-observability/user-guides/configure-logging.html#log-deduplication "
            "for more options.)"
        )
    else:
        return ""


def _color(msg: str) -> str:
    return "{}{}{}".format(colorama.Fore.GREEN, msg, colorama.Style.RESET_ALL)


stdout_deduplicator = LogDeduplicator(
    RAY_DEDUP_LOGS_AGG_WINDOW_S, RAY_DEDUP_LOGS_ALLOW_REGEX, RAY_DEDUP_LOGS_SKIP_REGEX
)
stderr_deduplicator = LogDeduplicator(
    RAY_DEDUP_LOGS_AGG_WINDOW_S, RAY_DEDUP_LOGS_ALLOW_REGEX, RAY_DEDUP_LOGS_SKIP_REGEX
)
