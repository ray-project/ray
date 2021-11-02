from base64 import b64encode
import logging
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional, Tuple

try:
    from pydantic import BaseModel
    from pydantic.main import ModelMetaclass
except ImportError:
    BaseModel = object
    ModelMetaclass = object

import requests

from ray._private.runtime_env.packaging import (
    create_package, get_uri_for_directory, parse_uri)
from ray._private.job_manager import JobStatus
from ray.dashboard.modules.job.data_types import (
    GetPackageRequest, GetPackageResponse, UploadPackageRequest, JobSpec,
    JobSubmitRequest, JobSubmitResponse, JobStatusRequest, JobStatusResponse,
    JobLogsRequest, JobLogsResponse)

from ray.dashboard.modules.job.job_head import (
    JOB_API_ROUTE_LOGS, JOB_API_ROUTE_SUBMIT, JOB_API_ROUTE_STATUS,
    JOB_API_ROUTE_PACKAGE)

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

    def _do_request(
            self,
            method: str,
            endpoint: str,
            data: BaseModel,
            response_type: Optional[ModelMetaclass] = None) -> Dict[Any, Any]:
        url = self._address + endpoint
        json_payload = data.dict()
        logger.debug(f"Sending request to {url} with payload {json_payload}.")
        r = requests.request(method, url, json=json_payload)
        r.raise_for_status()

        response_json = r.json()
        if not response_json["result"]:  # Indicates failure.
            raise Exception(response_json["msg"])

        if response_type is None:
            return None
        else:
            # Dashboard "framework" returns double-nested "data" field...
            return response_type(**response_json["data"]["data"])

    def _package_exists(self, package_uri: str) -> bool:
        req = GetPackageRequest(package_uri=package_uri)
        resp = self._do_request(
            "GET",
            JOB_API_ROUTE_PACKAGE,
            req,
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
            req = UploadPackageRequest(
                package_uri=package_uri,
                encoded_package_bytes=b64encode(package_file.read_bytes()))
            self._do_request("PUT", JOB_API_ROUTE_PACKAGE, req)
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
                   entrypoint: str,
                   runtime_env: Optional[Dict[str, Any]] = None,
                   metadata: Optional[Dict[str, str]] = None) -> str:
        runtime_env = runtime_env or {}
        metadata = metadata or {}

        self._upload_working_dir_if_needed(runtime_env)
        job_spec = JobSpec(
            entrypoint=entrypoint, runtime_env=runtime_env, metadata=metadata)
        req = JobSubmitRequest(job_spec=job_spec)
        resp = self._do_request("POST", JOB_API_ROUTE_SUBMIT, req,
                                JobSubmitResponse)
        return resp.job_id

    def get_job_status(self, job_id: str) -> JobStatus:
        req = JobStatusRequest(job_id=job_id)
        resp = self._do_request("GET", JOB_API_ROUTE_STATUS, req,
                                JobStatusResponse)
        return resp.job_status

    def get_job_logs(self, job_id: str) -> Tuple[str, str]:
        req = JobLogsRequest(job_id=job_id)
        resp = self._do_request("GET", JOB_API_ROUTE_LOGS, req,
                                JobLogsResponse)
        return resp.stdout, resp.stderr
