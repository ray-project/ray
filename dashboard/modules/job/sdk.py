import dataclasses
import logging
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional

import requests

from ray._private.runtime_env.packaging import (
    create_package, get_uri_for_directory, parse_uri)
from ray.dashboard.modules.job.common import (
    JobSubmitRequest, JobSubmitResponse, JobStopResponse, JobStatus,
    JobStatusResponse, JobLogsResponse, uri_to_http_components)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class JobSubmissionClient:
    def __init__(self, address: str):
        self._address: str = address.rstrip("/")
        self._test_connection()

    def _test_connection(self):
        try:
            assert not self._package_exists("gcs://FAKE_URI.zip")
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Failed to connect to Ray at address: {self._address}.")

    def _raise_error(self, r: requests.Response):
        raise RuntimeError(
            f"Request failed with status code {r.status_code}: {r.text}.")

    def _do_request(
            self,
            method: str,
            endpoint: str,
            *,
            data: Optional[bytes] = None,
            json_data: Optional[dict] = None,
    ) -> Optional[object]:
        url = self._address + endpoint
        logger.debug(
            f"Sending request to {url} with json data: {json_data or {}}.")
        return requests.request(method, url, data=data, json=json_data)

    def _package_exists(self, package_uri: str) -> bool:
        protocol, package_name = uri_to_http_components(package_uri)
        r = self._do_request("GET", f"/api/packages/{protocol}/{package_name}")

        if r.status_code == 200:
            logger.debug(f"Package {package_uri} already exists.")
            return True
        elif r.status_code == 404:
            logger.debug(f"Package {package_uri} does not exist.")
            return False
        else:
            self._raise_error(r)

    def _upload_package(self,
                        package_uri: str,
                        package_path: str,
                        include_parent_dir: Optional[bool] = False,
                        excludes: Optional[List[str]] = None) -> bool:
        logger.info(f"Uploading package {package_uri}.")
        with tempfile.TemporaryDirectory() as tmp_dir:
            protocol, package_name = uri_to_http_components(package_uri)
            package_file = Path(tmp_dir) / package_name
            create_package(
                package_path,
                package_file,
                include_parent_dir=include_parent_dir,
                excludes=excludes)
            try:
                r = self._do_request(
                    "PUT",
                    f"/api/packages/{protocol}/{package_name}",
                    data=package_file.read_bytes())
                if r.status_code != 200:
                    self._raise_error(r)
            finally:
                package_file.unlink()

    def _upload_package_if_needed(self,
                                  package_path: str,
                                  excludes: Optional[List[str]] = None) -> str:
        package_uri = get_uri_for_directory(package_path, excludes=excludes)
        if not self._package_exists(package_uri):
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

        logger.debug(f"Submitting job with job_id={job_id}.")
        r = self._do_request(
            "POST", "/api/jobs/", json_data=dataclasses.asdict(req))

        if r.status_code == 200:
            return JobSubmitResponse(**r.json()).job_id
        else:
            self._raise_error(r)

    def stop_job(self, job_id: str) -> bool:
        logger.debug(f"Stopping job with job_id={job_id}.")
        r = self._do_request(
            "POST",
            f"/api/jobs/{job_id}/stop",
        )

        if r.status_code == 200:
            return JobStopResponse(**r.json()).stopped
        else:
            self._raise_error(r)

    def get_job_status(self, job_id: str) -> JobStatus:
        r = self._do_request("GET", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            return JobStatusResponse(**r.json()).status
        else:
            self._raise_error(r)

    def get_job_logs(self, job_id: str) -> str:
        r = self._do_request("GET", f"/api/jobs/{job_id}/logs")

        if r.status_code == 200:
            return JobLogsResponse(**r.json()).logs
        else:
            self._raise_error(r)
