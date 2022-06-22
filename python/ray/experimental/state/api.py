import urllib
import warnings
import threading
import time
from dataclasses import fields
from typing import Dict, Generator, List, Optional, Tuple, Union, Any

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
    """State API Client issues REST GET requests to the server for resource states."""

    def __init__(
        self,
        address: Optional[str] = None,
        cookies: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
    ):
        """Initialize a StateApiClient and check the connection to the cluster.

        Args:
            address: The address of Ray API server. If not provided,
                it will be configured automatically from querying the GCS server.
            cookies: Cookies to use when sending requests to the HTTP job server.
            headers: Headers to use when sending requests to the HTTP job server, used
                for cases like authentication to a remote cluster.
        """
        if requests is None:
            raise RuntimeError(
                "The Ray state CLI & SDK require the ray[default] "
                "installation: `pip install 'ray[default']``"
            )
        if not headers:
            headers = {"Content-Type": "application/json"}
        super().__init__(
            address,
            create_cluster_if_needed=False,
            headers=headers,
            cookies=cookies,
        )

    def print_slow_api_warning(
        self,
        timeout: int,
        cv: threading.Condition,
        endpoint: str,
        print_interval_s: float,
        print_warning: bool,
    ):
        """Print the slow API warning. every print_interval_s seconds until timeout.

        Args:
            timeout: The API timeout.
            cv: Condition variable. If it is notified, the thread will terminate.
            endpoint: The endpoint to query.
            print_interval_s: The interval to print warning message until timeout.
            print_warning: Print the warning if it is set to True.
        """
        start = time.time()
        last = time.time()
        i = 1
        while True:
            with cv:
                # Will return True if it is notified from other thread
                # which means it should terminate the loop.
                # False if timeout is occured.
                notified_to_terminate = cv.wait(0.1)
                if notified_to_terminate:
                    break

            elapsed = time.time() - start
            if time.time() - last >= print_interval_s:
                if print_warning:
                    print(
                        f"({round(elapsed, 2)} / {timeout} seconds) "
                        "Waiting for the response from the API server "
                        f"address {self._address}{endpoint}."
                    )
                i += 1
                last = time.time()

            if time.time() - start > timeout:
                break

    def _request(
        self,
        endpoint: str,
        timeout: float,
        params: Dict,
        explain: bool,
        delay_warning_print_interval_s: float = 5,
    ):
        cv = threading.Condition(lock=threading.Lock())
        t = threading.Thread(
            target=self.print_slow_api_warning,
            args=(timeout, cv, endpoint, delay_warning_print_interval_s, explain),
        )
        t.start()

        response = None
        try:
            response = self._do_request(
                "GET",
                endpoint,
                timeout=timeout,
                params=params,
            )

            response.raise_for_status()
        except Exception as e:
            err_str = f"Failed to make request to {self._address}{endpoint}. "

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
        finally:
            with cv:
                cv.notify()
            t.join(timeout=1)

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

        response = self._request(endpoint, options.timeout, params, _explain)
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
        response = self._request(endpoint, options.timeout, params, _explain)

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
    address: The IP address and port of the head node. Defaults to
        http://localhost:8265.
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).list(
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
    return StateApiClient(address=address).summary(
        SummaryResource.TASKS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )


def summarize_actors(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(address=address).summary(
        SummaryResource.ACTORS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )


def summarize_objects(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return StateApiClient(address=address).summary(
        SummaryResource.OBJECTS,
        options=SummaryApiOptions(timeout=timeout),
        _explain=_explain,
    )
