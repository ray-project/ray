import dataclasses
import logging
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import requests

from ray._private.runtime_env.packaging import (
    create_package, get_uri_for_directory, parse_uri)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.data_types import (
    GetPackageResponse, JobSubmitRequest, JobSubmitResponse, JobStopResponse,
    JobStatusResponse, JobLogsResponse)

from ray.dashboard.modules.job.job_head import (
    JOBS_API_ROUTE_LOGS, JOBS_API_ROUTE_SUBMIT, JOBS_API_ROUTE_STOP,
    JOBS_API_ROUTE_STATUS, JOBS_API_ROUTE_PACKAGE)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobSubmissionClient:
    def __init__(self, address: str):
        self._address: str = address.rstrip("/")
        self._test_connection()

    def _test_connection(self):
        try:
            assert not self._package_exists("gcs://FAKE_URI")
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Failed to connect to Ray at address: {self._address}.")

    def _do_request(self,
                    method: str,
                    endpoint: str,
                    *,
                    data: Optional[bytes] = None,
                    json_data: Optional[dict] = None,
                    params: Optional[dict] = None,
                    response_type: Optional[type] = None) -> Optional[object]:
        url = self._address + endpoint
        logger.debug(f"Sending request to {url} with "
                     f"json: {json_data}, params: {params}.")
        r = requests.request(
            method, url, data=data, json=json_data, params=params)
        r.raise_for_status()
        if response_type is None:
            return None
        else:
            response = r.json()
            logger.info(f"Got response: {response}.")
            return response_type(**response)

    def _package_exists(self, package_uri: str) -> bool:
        resp = self._do_request(
            "GET",
            JOBS_API_ROUTE_PACKAGE,
            params={"package_uri": package_uri},
            response_type=GetPackageResponse)
        return resp.package_exists

    def _upload_package(self,
                        package_uri: str,
                        package_path: str,
                        include_parent_dir: Optional[bool] = False,
                        excludes: Optional[List[str]] = None) -> bool:
        with tempfile.TemporaryDirectory() as tmp_dir:
            package_name = parse_uri(package_uri)[1]
            package_file = Path(tmp_dir) / package_name
            create_package(
                package_path,
                package_file,
                include_parent_dir=include_parent_dir,
                excludes=excludes)
            self._do_request(
                "PUT",
                JOBS_API_ROUTE_PACKAGE,
                data=package_file.read_bytes(),
                params={"package_uri": package_uri})
            package_file.unlink()

    def _upload_package_if_needed(self,
                                  package_path: str,
                                  excludes: Optional[List[str]] = None) -> str:
        package_uri: str = get_uri_for_directory(
            package_path, excludes=excludes)
        if not self._package_exists(package_uri):
            logger.info(f"Uploading package {package_uri}.")
            self._upload_package(package_uri, package_path, excludes=excludes)
        else:
            logger.info(
                f"Package {package_uri} already exists, skipping upload.")

        return package_uri

    def _upload_working_dir_if_needed(self, runtime_env: Dict[str, Any]):
        if "working_dir" in runtime_env:
            working_dir = runtime_env["working_dir"]
            try:
                parse_uri(working_dir)
                is_uri = True
                logger.debug("working_dir is already a valid URI.")
            except ValueError:
                is_uri = False

            if not is_uri:
                logger.debug("working_dir is not a URI, attempting to upload.")
                package_uri = self._upload_package_if_needed(
                    working_dir, excludes=runtime_env.get("excludes", None))
                runtime_env["working_dir"] = package_uri

    def submit_job(self,
                   *,
                   entrypoint: str,
                   job_id: Optional[str] = None,
                   runtime_env: Optional[Dict[str, Any]] = None,
                   metadata: Optional[Dict[str, str]] = None) -> str:
        runtime_env = runtime_env or {}
        metadata = metadata or {}

        self._upload_working_dir_if_needed(runtime_env)
        req = JobSubmitRequest(
            entrypoint=entrypoint,
            job_id=job_id,
            runtime_env=runtime_env,
            metadata=metadata)
        resp = self._do_request(
            "POST",
            JOBS_API_ROUTE_SUBMIT,
            json_data=dataclasses.asdict(req),
            response_type=JobSubmitResponse)
        return resp.job_id

    def stop_job(self, job_id: str) -> bool:
        resp = self._do_request(
            "POST",
            JOBS_API_ROUTE_STOP,
            params={"job_id": job_id},
            response_type=JobStopResponse)
        return resp.stopped

    def get_job_status(self, job_id: str) -> JobStatus:
        resp = self._do_request(
            "GET",
            JOBS_API_ROUTE_STATUS,
            params={"job_id": job_id},
            response_type=JobStatusResponse)
        return resp.job_status

    def get_job_logs(self, job_id: str) -> Tuple[str, str]:
        resp = self._do_request(
            "GET",
            JOBS_API_ROUTE_LOGS,
            params={"job_id": job_id},
            response_type=JobLogsResponse)
        return resp.stdout, resp.stderr
