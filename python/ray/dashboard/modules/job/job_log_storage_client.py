import asyncio
import concurrent.futures
import os
import shutil
from typing import AsyncIterator, List, Optional, Tuple

import ray
from ray.dashboard.modules.job.common import JOB_LOGS_PATH_TEMPLATE
from ray.dashboard.modules.job.utils import fast_tail_last_n_lines, file_tail_iterator


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """

    # Number of last N lines to put in job message upon failure.
    NUM_LOG_LINES_ON_ERROR = 10
    # Maximum number of characters to print out of the logs to avoid
    # HUGE log outputs that bring down the api server
    MAX_LOG_SIZE = 20000

    def get_logs(self, job_id: str) -> str:
        log_path = self.get_log_file_path(job_id)
        contents = []
        for backup_path in self._get_rotated_backup_paths(log_path):
            try:
                with open(backup_path, "r") as f:
                    contents.append(f.read())
            except FileNotFoundError:
                pass
        try:
            with open(log_path, "r") as f:
                contents.append(f.read())
        except FileNotFoundError:
            pass
        return "".join(contents)

    def _get_rotated_backup_paths(self, log_path: str) -> List[str]:
        """Return rotated backup file paths for a log, oldest first.

        Backups are named {log_path}.1 (most recent) through
        {log_path}.N (oldest), matching the numbering used by
        rotate_log_file. This returns them in the order their content
        was actually written, oldest to newest, so callers can
        concatenate backups followed by the active file to reconstruct
        full log history across rotations.
        """
        backups = []
        i = 1
        while os.path.exists(f"{log_path}.{i}"):
            backups.append(f"{log_path}.{i}")
            i += 1
        return list(reversed(backups))

    def tail_logs(self, job_id: str) -> AsyncIterator[List[str]]:
        return file_tail_iterator(self.get_log_file_path(job_id))

    async def get_last_n_log_lines(
        self,
        job_id: str,
        num_log_lines: int = NUM_LOG_LINES_ON_ERROR,
        executor: Optional[concurrent.futures.Executor] = None,
    ) -> str:
        """Returns the last MAX_LOG_SIZE (20000) characters in the last ``num_log_lines`` lines.

        If the active log file was recently rotated and does not contain
        enough lines on its own, this falls back to also reading the most
        recent rotated backup file so a rotation boundary does not appear
        as a loss of log history right after it happens.

        This method's own body is entirely synchronous file I/O with no
        internal await points. By default it runs inline on the calling
        coroutine's thread, same as before this became executor-aware. If
        an executor is supplied, the work is submitted there instead and
        awaited via asyncio.wrap_future, so a large active log file (up
        to the configured max_bytes, 512MB by default) does not block
        the event loop of a caller that shares it with other work, e.g.
        JobSupervisor's actor loop. See PR #64528 discussion.

        Args:
            job_id: The id of the job whose logs we want to return
            num_log_lines: The number of lines to return.
            executor: Optional executor to run the synchronous file I/O
                on. If None, runs inline on the calling thread.

        Returns:
            Up to ``MAX_LOG_SIZE`` characters drawn from the last
            ``num_log_lines`` lines of the job's log file.
        """
        if executor is None:
            return self._get_last_n_log_lines_sync(job_id, num_log_lines)
        future = executor.submit(self._get_last_n_log_lines_sync, job_id, num_log_lines)
        return await asyncio.wrap_future(future)

    def _get_last_n_log_lines_sync(self, job_id: str, num_log_lines: int) -> str:
        """Synchronous implementation of get_last_n_log_lines.

        Kept as a plain method (not async) so it can be run directly on
        the calling thread or submitted to an executor unchanged. See
        get_last_n_log_lines for the public, executor-aware entry point.
        """
        log_path = self.get_log_file_path(job_id)
        try:
            with open(log_path, "r") as f:
                active_line_count = sum(1 for _ in f)
        except FileNotFoundError:
            active_line_count = 0

        if active_line_count >= num_log_lines:
            # Active file alone already has enough lines, no need to
            # look at backups.
            try:
                return fast_tail_last_n_lines(
                    path=log_path,
                    num_lines=num_log_lines,
                    max_chars=self.MAX_LOG_SIZE,
                )
            except FileNotFoundError:
                return ""

        # Active file has fewer lines than requested. Most likely a
        # rotation just happened, possibly truncating it all the way to
        # zero lines. Check backups before concluding there is nothing
        # to read: an empty active file does not necessarily mean the
        # job has produced no output, it may just mean rotation moved
        # that output into a backup file moments ago.
        backup_paths = self._get_rotated_backup_paths(log_path)
        if active_line_count == 0 and not backup_paths:
            # Genuinely nothing to read: job hasn't produced output yet,
            # or the log file doesn't exist at all.
            return ""

        remaining_lines = num_log_lines - active_line_count
        backup_text = ""
        if backup_paths:
            try:
                backup_text = fast_tail_last_n_lines(
                    path=backup_paths[-1],
                    num_lines=remaining_lines,
                    max_chars=self.MAX_LOG_SIZE,
                )
            except FileNotFoundError:
                pass

        with open(log_path, "r") as f:
            active_text = f.read()

        combined = backup_text + active_text
        if len(combined) > self.MAX_LOG_SIZE:
            combined = combined[-self.MAX_LOG_SIZE :]
        return combined

    def get_log_file_path(self, job_id: str) -> Tuple[str, str]:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            JOB_LOGS_PATH_TEMPLATE.format(submission_id=job_id),
        )

    def rotate_log_file(self, log_path: str, backup_count: int) -> None:
        """Rotate a job driver log file in place using copytruncate semantics.

        The driver subprocess holds this file open in append mode for the
        duration of the job, so we cannot rename the file out from under it
        the way standard log rotation does. Instead we copy the current
        content to a numbered backup file, then truncate the original file
        to zero length. Because the file is opened with O_APPEND, the
        subprocess's next write will land at the new end of file (offset 0)
        with no gap or corruption.

        A small window exists between reading the current content and
        truncating where a concurrent write from the driver could be lost.
        This is the same accepted tradeoff as `logrotate --copytruncate`.

        Args:
            log_path: Path to the job driver log file to rotate.
            backup_count: Max number of rotated backup files to keep.
                If <= 0, no backups are kept, current content is discarded.
        """
        if backup_count > 0:
            for i in range(backup_count - 1, 0, -1):
                src = f"{log_path}.{i}"
                dst = f"{log_path}.{i + 1}"
                if os.path.exists(src):
                    if os.path.exists(dst):
                        os.remove(dst)
                    os.rename(src, dst)
            if os.path.exists(log_path):
                shutil.copy2(log_path, f"{log_path}.1")

        try:
            with open(log_path, "r+") as f:
                f.truncate(0)
        except FileNotFoundError:
            # File may have been removed (e.g. job cleanup) between our
            # earlier os.path.exists check and this truncate. Nothing to
            # rotate in that case.
            pass
