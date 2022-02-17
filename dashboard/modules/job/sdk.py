import dataclasses
import logging
from typing import Any, Dict, Iterator, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

from ray.dashboard.modules.common import SubmissionClient
from ray.dashboard.modules.job.common import (
    JobSubmitRequest,
    JobSubmitResponse,
    JobStopResponse,
    JobStatusInfo,
    JobStatusResponse,
    JobLogsResponse,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobSubmissionClient(SubmissionClient):

    def submit_job(
        self,
        *,
        entrypoint: str,
        job_id: Optional[str] = None,
        runtime_env: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> str:
        runtime_env = runtime_env or {}
        metadata = metadata or {}
        metadata.update(self._default_metadata)

        self._upload_working_dir_if_needed(runtime_env)
        req = JobSubmitRequest(
            entrypoint=entrypoint,
            job_id=job_id,
            runtime_env=runtime_env,
            metadata=metadata,
        )

        logger.debug(f"Submitting job with job_id={job_id}.")
        r = self._do_request("POST", "/api/jobs/", json_data=dataclasses.asdict(req))

        if r.status_code == 200:
            return JobSubmitResponse(**r.json()).job_id
        else:
            self._raise_error(r)

    def stop_job(
        self,
        job_id: str,
    ) -> bool:
        logger.debug(f"Stopping job with job_id={job_id}.")
        r = self._do_request("POST", f"/api/jobs/{job_id}/stop")

        if r.status_code == 200:
            return JobStopResponse(**r.json()).stopped
        else:
            self._raise_error(r)

    def get_job_status(
        self,
        job_id: str,
    ) -> JobStatusInfo:
        r = self._do_request("GET", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            response = JobStatusResponse(**r.json())
            return JobStatusInfo(status=response.status, message=response.message)
        else:
            self._raise_error(r)

    def get_job_logs(self, job_id: str) -> str:
        r = self._do_request("GET", f"/api/jobs/{job_id}/logs")

        if r.status_code == 200:
            return JobLogsResponse(**r.json()).logs
        else:
            self._raise_error(r)

    async def tail_job_logs(self, job_id: str) -> Iterator[str]:
        async with aiohttp.ClientSession(cookies=self._cookies) as session:
            ws = await session.ws_connect(
                f"{self._address}/api/jobs/{job_id}/logs/tail"
            )

            while True:
                msg = await ws.receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    yield msg.data
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    pass
