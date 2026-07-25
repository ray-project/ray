import os
import shutil
from typing import AsyncIterator, List, Tuple

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
        try:
            with open(self.get_log_file_path(job_id), "r") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def tail_logs(self, job_id: str) -> AsyncIterator[List[str]]:
        return file_tail_iterator(self.get_log_file_path(job_id))

    async def get_last_n_log_lines(
        self, job_id: str, num_log_lines: int = NUM_LOG_LINES_ON_ERROR
    ) -> str:
        """Returns the last MAX_LOG_SIZE (20000) characters in the last ``num_log_lines`` lines.

        Args:
            job_id: The id of the job whose logs we want to return
            num_log_lines: The number of lines to return.

        Returns:
            Up to ``MAX_LOG_SIZE`` characters drawn from the last
            ``num_log_lines`` lines of the job's log file.
        """
        return fast_tail_last_n_lines(
            path=self.get_log_file_path(job_id),
            num_lines=num_log_lines,
            max_chars=self.MAX_LOG_SIZE,
        )

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

        with open(log_path, "r+") as f:
            f.truncate(0)
