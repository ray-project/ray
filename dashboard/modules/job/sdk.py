import dataclasses
import importlib
import logging
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None

from ray._private.runtime_env.packaging import (
    create_package, get_uri_for_directory, parse_uri)
from ray.dashboard.modules.job.common import (
    JobSubmitRequest, JobSubmitResponse, JobStopResponse, JobStatusInfo,
    JobStatusResponse, JobLogsResponse, uri_to_http_components)

from ray.client_builder import _split_address

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclasses.dataclass
class ClusterInfo:
    address: str
    cookies: Optional[Dict[str, Any]]
    metadata: Optional[Dict[str, Any]]


def get_job_submission_client_cluster_info(
        address: str, create_cluster_if_needed: bool) -> ClusterInfo:
    """Get address, cookies, and metadata used for JobSubmissionClient.

        Args:
            address (str): Address without the module prefix that is passed
                to JobSubmissionClient.
            create_cluster_if_needed (bool): Indicates whether the cluster
                of the address returned needs to be running. Ray doesn't
                start a cluster before interacting with jobs, but other
                implementations may do so.

        Returns:
            ClusterInfo object consisting of address, cookies, and metadata
            for JobSubmissionClient to use.
        """
    return ClusterInfo(
        address="http://" + address, cookies=None, metadata=None)


def parse_cluster_info(address: str,
                       create_cluster_if_needed: bool) -> ClusterInfo:
    module_string, inner_address = _split_address(address.rstrip("/"))

    # If user passes in a raw HTTP(S) address, just pass it through.
    if module_string == "http" or module_string == "https":
        return ClusterInfo(address=address, cookies=None, metadata=None)
    # If user passes in a Ray address, convert it to HTTP.
    elif module_string == "ray":
        return get_job_submission_client_cluster_info(
            inner_address, create_cluster_if_needed)
    # Try to dynamically import the function to get cluster info.
    else:
        try:
            module = importlib.import_module(module_string)
        except Exception:
            raise RuntimeError(
                f"Module: {module_string} does not exist.\n"
                f"This module was parsed from Address: {address}") from None
        assert "get_job_submission_client_cluster_info" in dir(module), (
            f"Module: {module_string} does "
            "not have `get_job_submission_client_cluster_info`.")

        return module.get_job_submission_client_cluster_info(
            inner_address, create_cluster_if_needed)


class JobSubmissionClient:
    def __init__(self, address: str, create_cluster_if_needed=False):
        if requests is None:
            raise RuntimeError(
                "The Ray jobs CLI & SDK require the ray[default] "
                "installation: `pip install 'ray[default']``")

        cluster_info = parse_cluster_info(address, create_cluster_if_needed)
        self._address = cluster_info.address
        self._cookies = cluster_info.cookies
        self._default_metadata = cluster_info.metadata or {}

        self._check_connection_and_version()

    def _check_connection_and_version(self):
        try:
            r = self._do_request("GET", "/api/version")
            if r.status_code == 404:
                raise RuntimeError(
                    "Jobs API not supported on the Ray cluster. "
                    "Please ensure the cluster is running "
                    "Ray 1.9 or higher.")

            r.raise_for_status()
            # TODO(edoakes): check the version if/when we break compatibility.
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Failed to connect to Ray at address: {self._address}.")

    def _raise_error(self, r: "requests.Response"):
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
        return requests.request(
            method, url, cookies=self._cookies, data=data, json=json_data)

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
        metadata.update(self._default_metadata)

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

    def get_job_status(self, job_id: str) -> JobStatusInfo:
        r = self._do_request("GET", f"/api/jobs/{job_id}")

        if r.status_code == 200:
            response = JobStatusResponse(**r.json())
            return JobStatusInfo(
                status=response.status, message=response.message)
        else:
            self._raise_error(r)

    def get_job_logs(self, job_id: str) -> str:
        r = self._do_request("GET", f"/api/jobs/{job_id}/logs")

        if r.status_code == 200:
            return JobLogsResponse(**r.json()).logs
        else:
            self._raise_error(r)
