import dataclasses
import importlib
import logging
import json
import yaml
from pathlib import Path
import tempfile
from typing import Any, Dict, List, Optional
from pkg_resources import packaging

try:
    import aiohttp
    import requests
except ImportError:
    aiohttp = None
    requests = None

from ray._private.runtime_env.packaging import (
    create_package,
    get_uri_for_directory,
    get_uri_for_package,
)
from ray._private.runtime_env.py_modules import upload_py_modules_if_needed
from ray._private.runtime_env.working_dir import upload_working_dir_if_needed
from ray.dashboard.modules.job.common import uri_to_http_components

from ray.util.annotations import PublicAPI
from ray.client_builder import _split_address
from ray.autoscaler._private.cli_logger import cli_logger

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# By default, connect to local cluster.
DEFAULT_DASHBOARD_ADDRESS = "http://localhost:8265"


def parse_runtime_env_args(
    runtime_env: Optional[str] = None,
    runtime_env_json: Optional[str] = None,
    working_dir: Optional[str] = None,
):
    """
    Generates a runtime_env dictionary using `runtime_env`, `runtime_env_json`,
    and `working_dir` CLI options. Only one of `runtime_env` or
    `runtime_env_json` may be defined. `working_dir` overwrites the
    `working_dir` from any other option.
    """

    final_runtime_env = {}
    if runtime_env is not None:
        if runtime_env_json is not None:
            raise ValueError(
                "Only one of --runtime_env and --runtime-env-json can be provided."
            )
        with open(runtime_env, "r") as f:
            final_runtime_env = yaml.safe_load(f)

    elif runtime_env_json is not None:
        final_runtime_env = json.loads(runtime_env_json)

    if working_dir is not None:
        if "working_dir" in final_runtime_env:
            cli_logger.warning(
                "Overriding runtime_env working_dir with --working-dir option"
            )

        final_runtime_env["working_dir"] = working_dir

    return final_runtime_env


@dataclasses.dataclass
class ClusterInfo:
    address: str
    cookies: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None


# TODO (shrekris-anyscale): renaming breaks compatibility, do NOT rename
def get_job_submission_client_cluster_info(
    address: str,
    # For backwards compatibility
    *,
    # only used in importlib case in parse_cluster_info, but needed
    # in function signature.
    create_cluster_if_needed: Optional[bool] = False,
    cookies: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    _use_tls: Optional[bool] = False,
) -> ClusterInfo:
    """Get address, cookies, and metadata used for SubmissionClient.

    If no port is specified in `address`, the Ray dashboard default will be
    inserted.

    Args:
        address (str): Address without the module prefix that is passed
            to SubmissionClient.
        create_cluster_if_needed (bool): Indicates whether the cluster
            of the address returned needs to be running. Ray doesn't
            start a cluster before interacting with jobs, but other
            implementations may do so.

    Returns:
        ClusterInfo object consisting of address, cookies, and metadata
        for SubmissionClient to use.
    """

    scheme = "https" if _use_tls else "http"
    return ClusterInfo(
        address=f"{scheme}://{address}",
        cookies=cookies,
        metadata=metadata,
        headers=headers,
    )


def parse_cluster_info(
    address: Optional[str] = None,
    create_cluster_if_needed: bool = False,
    cookies: Optional[Dict[str, Any]] = None,
    metadata: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
) -> ClusterInfo:
    if address is None:
        logger.info(f"No address provided, defaulting to {DEFAULT_DASHBOARD_ADDRESS}.")
        address = DEFAULT_DASHBOARD_ADDRESS

    module_string, inner_address = _split_address(address)

    # If user passes in ray://, raise error. Dashboard submission should
    # not use a Ray client address.
    if module_string == "ray":
        raise ValueError(
            f'Got an unexpected Ray client address "{address}" while trying '
            "to connect to the Ray dashboard. The dashboard SDK requires the "
            "Ray dashboard server's HTTP(S) address (which should start with "
            '"http://" or "https://", not "ray://"). If this address '
            "wasn't passed explicitly, it may be set in the RAY_ADDRESS "
            "environment variable."
        )

    # If user passes http(s)://, go through normal parsing.
    if module_string in {"http", "https"}:
        return get_job_submission_client_cluster_info(
            inner_address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
            _use_tls=module_string == "https",
        )
    # Try to dynamically import the function to get cluster info.
    else:
        try:
            module = importlib.import_module(module_string)
        except Exception:
            raise RuntimeError(
                f"Module: {module_string} does not exist.\n"
                f"This module was parsed from Address: {address}"
            ) from None
        assert "get_job_submission_client_cluster_info" in dir(module), (
            f"Module: {module_string} does "
            "not have `get_job_submission_client_cluster_info`."
        )

        return module.get_job_submission_client_cluster_info(
            inner_address,
            create_cluster_if_needed=create_cluster_if_needed,
            cookies=cookies,
            metadata=metadata,
            headers=headers,
        )


class SubmissionClient:
    def __init__(
        self,
        address: Optional[str] = None,
        create_cluster_if_needed: bool = False,
        cookies: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):

        cluster_info = parse_cluster_info(
            address, create_cluster_if_needed, cookies, metadata, headers
        )
        self._address = cluster_info.address
        self._cookies = cluster_info.cookies
        self._default_metadata = cluster_info.metadata or {}
        # Headers used for all requests sent to job server, optional and only
        # needed for cases like authentication to remote cluster.
        self._headers = cluster_info.headers

    def _check_connection_and_version(
        self, min_version: str = "1.9", version_error_message: str = None
    ):
        if version_error_message is None:
            version_error_message = (
                f"Please ensure the cluster is running Ray {min_version} or higher."
            )

        try:
            r = self._do_request("GET", "/api/version")
            if r.status_code == 404:
                raise RuntimeError(version_error_message)
            r.raise_for_status()

            running_ray_version = r.json()["ray_version"]
            if packaging.version.parse(running_ray_version) < packaging.version.parse(
                min_version
            ):
                raise RuntimeError(version_error_message)
            # TODO(edoakes): check the version if/when we break compatibility.
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"Failed to connect to Ray at address: {self._address}."
            )

    def _raise_error(self, r: "requests.Response"):
        raise RuntimeError(
            f"Request failed with status code {r.status_code}: {r.text}."
        )

    def _do_request(
        self,
        method: str,
        endpoint: str,
        *,
        data: Optional[bytes] = None,
        json_data: Optional[dict] = None,
    ) -> "requests.Response":
        url = self._address + endpoint
        logger.debug(f"Sending request to {url} with json data: {json_data or {}}.")
        return requests.request(
            method,
            url,
            cookies=self._cookies,
            data=data,
            json=json_data,
            headers=self._headers,
        )

    def _package_exists(
        self,
        package_uri: str,
    ) -> bool:
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

    def _upload_package(
        self,
        package_uri: str,
        package_path: str,
        include_parent_dir: Optional[bool] = False,
        excludes: Optional[List[str]] = None,
        is_file: bool = False,
    ) -> bool:
        logger.info(f"Uploading package {package_uri}.")
        with tempfile.TemporaryDirectory() as tmp_dir:
            protocol, package_name = uri_to_http_components(package_uri)
            if is_file:
                package_file = Path(package_path)
            else:
                package_file = Path(tmp_dir) / package_name
                create_package(
                    package_path,
                    package_file,
                    include_parent_dir=include_parent_dir,
                    excludes=excludes,
                )
            try:
                r = self._do_request(
                    "PUT",
                    f"/api/packages/{protocol}/{package_name}",
                    data=package_file.read_bytes(),
                )
                if r.status_code != 200:
                    self._raise_error(r)
            finally:
                # If the package is a user's existing file, don't delete it.
                if not is_file:
                    package_file.unlink()

    def _upload_package_if_needed(
        self,
        package_path: str,
        include_parent_dir: bool = False,
        excludes: Optional[List[str]] = None,
        is_file: bool = False,
    ) -> str:
        if is_file:
            package_uri = get_uri_for_package(Path(package_path))
        else:
            package_uri = get_uri_for_directory(package_path, excludes=excludes)

        if not self._package_exists(package_uri):
            self._upload_package(
                package_uri,
                package_path,
                include_parent_dir=include_parent_dir,
                excludes=excludes,
                is_file=is_file,
            )
        else:
            logger.info(f"Package {package_uri} already exists, skipping upload.")

        return package_uri

    def _upload_working_dir_if_needed(self, runtime_env: Dict[str, Any]):
        def _upload_fn(working_dir, excludes, is_file=False):
            self._upload_package_if_needed(
                working_dir,
                include_parent_dir=False,
                excludes=excludes,
                is_file=is_file,
            )

        upload_working_dir_if_needed(runtime_env, upload_fn=_upload_fn)

    def _upload_py_modules_if_needed(self, runtime_env: Dict[str, Any]):
        def _upload_fn(module_path, excludes, is_file=False):
            self._upload_package_if_needed(
                module_path, include_parent_dir=True, excludes=excludes, is_file=is_file
            )

        upload_py_modules_if_needed(runtime_env, upload_fn=_upload_fn)

    @PublicAPI(stability="beta")
    def get_version(self) -> str:
        r = self._do_request("GET", "/api/version")
        if r.status_code == 200:
            return r.json().get("version")
        else:
            self._raise_error(r)
