import os
from typing import AsyncIterator, List, Tuple

import ray
from ray.dashboard.modules.job.common import JOB_LOGS_PATH_TEMPLATE
from ray.dashboard.modules.job.utils import fast_tail_last_n_lines, file_tail_iterator

# Maximum bytes to read from a job log file via get_logs().
# Prevents the dashboard agent from OOMing when a single job produces
# tens of gigabytes of stdout/stderr.  Override with the environment
# variable RAY_JOB_LOG_MAX_READ_BYTES (integer, in bytes).
JOB_LOG_MAX_READ_BYTES = int(
    os.environ.get("RAY_JOB_LOG_MAX_READ_BYTES", 16 * 1024 * 1024)
)


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
            log_path = self.get_log_file_path(job_id)
            file_size = os.path.getsize(log_path)

            if file_size <= JOB_LOG_MAX_READ_BYTES:
                with open(log_path, "r") as f:
                    return f.read()

            # File exceeds the cap — return only the tail to bound memory.
            with open(log_path, "r") as f:
                f.seek(file_size - JOB_LOG_MAX_READ_BYTES)
                f.readline()  # skip partial first line
                tail = f.read()

            total_human = (
                f"{file_size / (1024**3):.1f} GiB"
                if file_size >= 1024**3
                else f"{file_size / (1024**2):.1f} MiB"
            )
            cap_human = f"{JOB_LOG_MAX_READ_BYTES / (1024**2):.0f} MiB"
            return (
                f"[LOG TRUNCATED — showing last {cap_human} "
                f"of {total_human} total]\n{tail}"
            )
        except FileNotFoundError:
            return ""

    def tail_logs(self, job_id: str) -> AsyncIterator[List[str]]:
        return file_tail_iterator(self.get_log_file_path(job_id))

    async def get_last_n_log_lines(
        self, job_id: str, num_log_lines=NUM_LOG_LINES_ON_ERROR
    ) -> str:
        """
        Returns the last MAX_LOG_SIZE (20000) characters in the last
        `num_log_lines` lines.

        Args:
            job_id: The id of the job whose logs we want to return
            num_log_lines: The number of lines to return.
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
