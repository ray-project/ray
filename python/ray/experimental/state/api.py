import urllib
import warnings
from dataclasses import fields
from typing import Dict, Generator, List, Optional, Tuple, Union

import requests

import ray
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.experimental.state.common import (
    SummaryApiOptions,
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    GetLogOptions,
    ListApiOptions,
    StateResource,
    SupportedFilterType,
    SummaryResource,
)
from ray.experimental.state.exception import RayStateApiException, ServerUnavailable

"""
This file contains API client and methods for querying ray state.

NOTE(rickyyx): This is still a work-in-progress API, and subject to changes.

Usage:
    1. [Recommended] With StateApiClient:
    ```
        client = StateApiClient(api_server_address="localhost:8265")
        data = client.list(StateResource.NODES)
        ...
    ```

    2. With SDK APIs:
    The API creates a `StateApiClient` for each invocation. So if multiple
    invocations of listing are used, it is better to reuse the `StateApiClient`
    as suggested above.
    ```
        data = list_nodes(address="localhost:8265")
    ```
"""


class StateApiClient(SubmissionClient):
    """State API Client issues REST GET requests to the server for resource states.

    Args:
        api_server_address: The address of API server. If it is not give, it assumes
        the ray is already connected and obtains the API server address using Ray API.
    """

    def __init__(
        self,
        api_server_address: str = None,
    ):
        super().__init__(
            self._get_default_api_server_address()
            if api_server_address is None
            else api_server_address,
            create_cluster_if_needed=False,
            headers={"Content-Type": "application/json"},
        )

    @classmethod
    def _get_default_api_server_address(cls) -> str:
        assert (
            ray.is_initialized()
            and ray._private.worker.global_worker.node.address_info["webui_url"]
            is not None
        )
        return (
            f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
        )

    def _request(self, endpoint: str, timeout: float, params: Dict):
        try:
            response = self._do_request(
                "GET",
                endpoint,
                timeout=timeout,
                params=params,
            )

            response.raise_for_status()
        except Exception as e:
            err_str = f"Failed to make request to {endpoint}. "

            # Best-effort to give hints to users on potential reasons of connection
            # failure.
            if isinstance(e, requests.exceptions.ConnectionError):
                err_str += (
                    "Failed to connect to API server. Please check the API server "
                    "log for details. Make sure dependencies are installed with "
                    "`pip install ray[default]`."
                )
                raise ServerUnavailable(err_str)

            if response is not None:
                err_str += f"Response(url={response.url},status={response.status_code})"
            raise RayStateApiException(err_str) from e

        response = response.json()
        return response

    def list(
        self, resource: StateResource, options: ListApiOptions, _explain: bool = False
    ) -> Union[Dict, List]:
        """List resources states

        Args:
            resource_name: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            options: List options. See `ListApiOptions` for details.
            _explain: Print the API information such as API
                latency or failed query information.

        Returns:
            A list of queried result from `ListApiResponse`,

        Raises:
            This doesn't catch any exceptions raised when the underlying request
            call raises exceptions. For example, it could raise `requests.Timeout`
            when timeout occurs.

        """
        endpoint = f"/api/v0/{resource.value}"

        # We don't use `asdict` to avoid deepcopy.
        # https://docs.python.org/3/library/dataclasses.html#dataclasses.asdict
        params = {
            "limit": options.limit,
            "timeout": options.timeout,
            "filter_keys": [],
            "filter_values": [],
        }
        for filter in options.filters:
            filter_k, filter_val = filter
            params["filter_keys"].append(filter_k)
            params["filter_values"].append(filter_val)

        response = self._request(endpoint, options.timeout, params)
        if response["result"] is False:
            raise RayStateApiException(
                "API server internal error. See dashboard.log file for more details. "
                f"Error: {response['msg']}"
            )

        # Print warnings if anything was given.
        warning_msgs = response["data"].get("partial_failure_warning", None)
        if warning_msgs and _explain:
            warnings.warn(warning_msgs, RuntimeWarning)

        return response["data"]["result"]

    def summary(
        self,
        resource: SummaryResource,
        *,
        options: SummaryApiOptions,
        _explain: bool = False,
    ) -> Dict:
        """Summarize resources states

        Args:
            resource_name: Resource names,
                see `SummaryResource` for details.
            options: summary options. See `SummaryApiOptions` for details.
            _explain: Print the API information such as API
                latency or failed query information.

        Returns:
            A dictionary of queried result from `SummaryApiResponse`,

        Raises:
            This doesn't catch any exceptions raised when the underlying request
            call raises exceptions. For example, it could raise `requests.Timeout`
            when timeout occurs.
        """
        params = {"timeout": options.timeout}
        endpoint = f"/api/v0/{resource.value}/summarize"
        response = self._request(endpoint, options.timeout, params)

        if response["result"] is False:
            raise RayStateApiException(
                "API server internal error. See dashboard.log file for more details. "
                f"Error: {response['msg']}"
            )
        if _explain:
            # Print warnings if anything was given.
            warning_msg = response["data"].get("partial_failure_warning", None)
            if warning_msg:
                warnings.warn(warning_msg, RuntimeWarning)

        return response["data"]["result"]["node_id_to_summary"]


"""
Convenient methods for list_<RESOURCE>

Supported arguments to the below methods, see `ListApiOptions`:
    address: The address of the Ray state server. If None, it assumes a running Ray
        deployment exists and will query the GCS for auto-configuration.
    filters: Optional list of filter key-value pair.
    timeout: Time for the request.
    limit: Limit of entries in the result
"""


def list_actors(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.ACTORS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_placement_groups(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.PLACEMENT_GROUPS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_nodes(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.NODES,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_jobs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.JOBS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_workers(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.WORKERS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_tasks(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.TASKS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_objects(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.OBJECTS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


def list_runtime_envs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.RUNTIME_ENVS,
        options=ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        _explain=_explain,
    )


"""
Log APIs
"""


def get_log(
    api_server_url: str = None,
    node_id: Optional[str] = None,
    node_ip: Optional[str] = None,
    filename: Optional[str] = None,
    actor_id: Optional[str] = None,
    task_id: Optional[str] = None,
    pid: Optional[int] = None,
    follow: bool = False,
    tail: int = 100,
    _interval: Optional[float] = None,
) -> Generator[str, None, None]:
    if api_server_url is None:
        assert ray.is_initialized()
        api_server_url = (
            f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
        )

    media_type = "stream" if follow else "file"
    options = GetLogOptions(
        node_id=node_id,
        node_ip=node_ip,
        filename=filename,
        actor_id=actor_id,
        task_id=task_id,
        pid=pid,
        lines=tail,
        interval=_interval,
        media_type=media_type,
        timeout=DEFAULT_RPC_TIMEOUT,
    )
    options_dict = {}
    for field in fields(options):
        option_val = getattr(options, field.name)
        if option_val:
            options_dict[field.name] = option_val

    with requests.get(
        f"{api_server_url}/api/v0/logs/{media_type}?"
        f"{urllib.parse.urlencode(options_dict)}",
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise RayStateApiException(r.text)
        for bytes in r.iter_content(chunk_size=None):
            bytes = bytearray(bytes)
            # First byte 1 means success.
            if bytes.startswith(b"1"):
                bytes.pop(0)
                logs = bytes.decode("utf-8")
            else:
                assert bytes.startswith(b"0")
                error_msg = bytes.decode("utf-8")
                raise RayStateApiException(error_msg)
            yield logs


def list_logs(
    api_server_url: str = None,
    node_id: str = None,
    node_ip: str = None,
    glob_filter: str = None,
) -> Dict[str, List[str]]:
    if api_server_url is None:
        assert ray.is_initialized()
        api_server_url = (
            f"http://{ray._private.worker.global_worker.node.address_info['webui_url']}"
        )

    if not glob_filter:
        glob_filter = "*"

    options_dict = {}
    if node_ip:
        options_dict["node_ip"] = node_ip
    if node_id:
        options_dict["node_id"] = node_id
    if glob_filter:
        options_dict["glob"] = glob_filter

    r = requests.get(
        f"{api_server_url}/api/v0/logs?{urllib.parse.urlencode(options_dict)}"
    )
    r.raise_for_status()

    response = r.json()
    if response["result"] is False:
        raise RayStateApiException(
            "API server internal error. See dashboard.log file for more details. "
            f"Error: {response['msg']}"
        )
    return response["data"]["result"]


"""
Summary APIs
"""


def summarize_tasks(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).summary(
        SummaryResource.TASKS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )


def summarize_actors(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).summary(
        SummaryResource.ACTORS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )


def summarize_objects(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).summary(
        SummaryResource.OBJECTS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )
