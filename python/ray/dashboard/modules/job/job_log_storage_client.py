import os
from collections import deque
from typing import AsyncIterator, List, Tuple

import ray
from ray.dashboard.modules.job.common import JOB_LOGS_PATH_TEMPLATE
from ray.dashboard.modules.job.utils import (
    file_tail_iterator,
    encrypt_aes,
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

    def get_logs(self, job_id: str, err_log=False) -> str:
        job_path = self.get_log_file_path(job_id)
        if err_log:
            job_path = self.get_err_file_path(job_id)

        try:
            with open(job_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            return ""

    def tail_logs(self, job_id: str, err_log=False) -> AsyncIterator[List[str]]:
        job_path = self.get_log_file_path(job_id)
        if err_log:
            job_path = self.get_err_file_path(job_id)
        return file_tail_iterator(job_path)

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
        log_tail_deque = deque(maxlen=num_log_lines)
        async for lines in self.tail_logs(job_id):
            if lines is None:
                break
            else:
                # log_tail_iter can return batches of lines at a time.
                for line in lines:
                    log_tail_deque.append(line)

        return "".join(log_tail_deque)[-self.MAX_LOG_SIZE :]

    def get_log_file_path(self, job_id: str) -> Tuple[str, str]:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            JOB_LOGS_PATH_TEMPLATE.format(submission_id=job_id),
        )

    def get_err_file_path(self, job_id: str) -> Tuple[str, str]:
        """
        Get the file path to the err logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.err
        """
        return os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            self.JOB_ERR_LOGS_PATH.format(job_id=job_id),
        )

    def get_log_agent_file_path(
        self, job_id: str, log_agent_link: str, encrypt_key: str
    ) -> Tuple[str, str]:
        """
        Get the file path to the logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.log
        """
        if log_agent_link is None or encrypt_key is None:
            return None

        psm = os.environ.get("TCE_PSM")
        hostip = os.environ.get("BYTED_RAY_POD_IP")
        podname = os.environ.get("MY_POD_NAME")
        containername = (
            "ray-head" if os.environ.get("RAY_IP") == "127.0.0.1" else "worker"
        )
        logname = os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            self.JOB_LOGS_PATH.format(job_id=job_id),
        )

        if psm is None or hostip is None or podname is None or containername is None:
            return None

        if hostip.startswith("[") and hostip.endswith("]"):
            hostip = hostip[1:-1]

        params = (
            f"psm={psm}&hostip={hostip}&podname={podname}&containername={containername}"
        )
        if logname is not None:
            params = f"{params}&logname={logname}"
        params = f"{params}&username=xxx"

        code = encrypt_aes(encrypt_key, params)
        if code is None:
            return None
        return f"{log_agent_link}code={code.decode('utf-8')}"

    def get_err_agent_file_path(
        self, job_id: str, log_agent_link: str, encrypt_key: str
    ) -> Tuple[str, str]:
        """
        Get the file path to the err logs of a given job. Example:
            /tmp/ray/session_date/logs/job-driver-{job_id}.err
        """
        if log_agent_link is None or encrypt_key is None:
            return None

        psm = os.environ.get("TCE_PSM")
        hostip = os.environ.get("BYTED_RAY_POD_IP")
        podname = os.environ.get("MY_POD_NAME")
        containername = (
            "ray-head" if os.environ.get("RAY_IP") == "127.0.0.1" else "worker"
        )
        logname = os.path.join(
            ray._private.worker._global_node.get_logs_dir_path(),
            self.JOB_ERR_LOGS_PATH.format(job_id=job_id),
        )

        if psm is None or hostip is None or podname is None or containername is None:
            return None

        if hostip.startswith("[") and hostip.endswith("]"):
            hostip = hostip[1:-1]

        params = (
            f"psm={psm}&hostip={hostip}&podname={podname}&containername={containername}"
        )
        if logname is not None:
            params = f"{params}&logname={logname}"
        params = f"{params}&username=xxx"

        code = encrypt_aes(encrypt_key, params)
        if code is None:
            return None
        return f"{log_agent_link}code={code.decode('utf-8')}"

    # BYTEDANCE INTERNAL
