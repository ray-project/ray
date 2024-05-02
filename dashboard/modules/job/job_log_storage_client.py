import asyncio
import os
from collections import deque
from typing import List, Iterator

import ray
from ray.util.client.common import INT32_MAX
from ray.dashboard.modules.job.common import (
    JOB_LOGS_PATH_TEMPLATE,
)
from ray.dashboard.modules.job.utils import file_tail_iterator


class JobLogStorageClient:
    """
    Disk storage for stdout / stderr of driver script logs.
    """

    # Number of last N lines to put in job message upon failure.
    MAX_LOG_LINES_ON_ERROR = 10
    # Max number of characters to print out of the logs to avoid
    # HUGE log outputs that bring down the api server
    MAX_LOG_SNIPPET_SIZE_ON_ERROR = 20000
    # Max number of lines being fetched from log file per chunk (after
    # every chunk event-loop has to be yielded to make sure that other tasks
    # are not being starved)
    MAX_LINES_PER_CHUNK = 10

    def tail_logs(
        self,
        job_id: str,
        *,
        max_lines_per_chunk: int = MAX_LINES_PER_CHUNK,
    ) -> Iterator[List[str]]:
        return file_tail_iterator(
            self.get_log_file_path(job_id),
            max_lines_per_chunk=max_lines_per_chunk,
        )

    async def get_logs(
        self,
        job_id: str,
        *,
        max_log_lines: int = INT32_MAX,  # unbounded
        max_total_size: int = INT32_MAX,  # unbounded
    ) -> str:
        """
        Returns the last MAX_LOG_SIZE (20000) characters in the last
        `num_log_lines` lines.

        Args:
            job_id: The id of the job whose logs we want to return
            max_log_lines: The number of lines to return.
        """
        log_tail_deque = deque(maxlen=max_log_lines)

        max_lines_per_chunk = min(self.MAX_LINES_PER_CHUNK, max_log_lines)
        read_lines_count = 0

        for lines in self.tail_logs(job_id, max_lines_per_chunk=max_lines_per_chunk):
            if lines is None:
                break

            # log_tail_iter can return batches of lines at a time.
            for line in lines:
                log_tail_deque.append(line)

            read_lines_count += len(lines)

            # NOTE: We have to yield the event-loop after every chunk being read
            #       to make sure that log tailing is not blocking the event-loop
            if read_lines_count % max_lines_per_chunk == 1:
                await asyncio.sleep(0)

        return "".join(log_tail_deque)[-max_total_size:]

    @staticmethod
    def get_log_file_path(job_id: str) -> str:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            JOB_LOGS_PATH_TEMPLATE.format(submission_id=job_id),
        )