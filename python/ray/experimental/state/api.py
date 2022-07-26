import logging
import threading
import urllib
import warnings
from contextlib import contextmanager
from dataclasses import fields
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import requests

import ray
from ray.dashboard.modules.dashboard_sdk import SubmissionClient
from ray.experimental.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_LOG_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    ActorState,
    GetApiOptions,
    GetLogOptions,
    JobState,
    ListApiOptions,
    NodeState,
    ObjectState,
    PlacementGroupState,
    PredicateType,
    StateResource,
    SummaryApiOptions,
    SummaryResource,
    SupportedFilterType,
    TaskState,
    WorkerState,
)
from ray.experimental.state.exception import RayStateApiException, ServerUnavailable

logger = logging.getLogger(__name__)


@contextmanager
def warnings_on_slow_request(
    *, address: str, endpoint: str, timeout: float, explain: bool
):
    """A context manager to print warnings if the request is replied slowly.

    Warnings are printed 3 times

    Args:
        address: The address of the endpoint.
        endpoint: The name of the endpoint.
        timeout: Request timeout in seconds.
        explain: Whether ot not it will print the warning.
    """
    # Do nothing if explain is not specified.
    if not explain:
        yield
        return

    # Prepare timers to print warning.
    # Print 3 times with exponential backoff. timeout / 2, timeout / 4, timeout / 8
    def print_warning(elapsed: float):
        logger.info(
            f"({round(elapsed, 2)} / {timeout} seconds) "
            "Waiting for the response from the API server "
            f"address {address}{endpoint}.",
        )

    warning_timers = [
        threading.Timer(timeout / i, print_warning, args=[timeout / i])
        for i in [2, 4, 8]
    ]

    try:
        for timer in warning_timers:
            timer.start()
        yield
    finally:
        # Make sure all timers are cancelled once request is terminated.
        for timer in warning_timers:
            timer.cancel()


"""
This file contains API client and methods for querying ray state.

NOTE(rickyyx): This is still a work-in-progress API, and subject to changes.

If you have any feedback, you could do so at either way as below:
  1. Report bugs/issues with details: https://forms.gle/gh77mwjEskjhN8G46 ,
  2. Follow up in #ray-state-observability-dogfooding slack channel of Ray:
    https://tinyurl.com/2pm26m4a"


Usage:
    1. [Recommended] With StateApiClient:
    ```
        client = StateApiClient(address="localhost:8265")
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

    @classmethod
    def _make_param(cls, options: Union[ListApiOptions, GetApiOptions]) -> Dict:
        options_dict = {}
        for field in fields(options):
            # TODO(rickyyx): We will need to find a way to pass server side timeout
            # TODO(rickyyx): We will have to convert filter option
            # slightly differently for now. But could we do k,v pair rather than this?
            # I see we are also converting dict to XXXApiOptions later on, we could
            # probably organize the marshaling a bit better.
            if field.name == "filters":
                options_dict["filter_keys"] = []
                options_dict["filter_predicates"] = []
                options_dict["filter_values"] = []
                for filter in options.filters:
                    if len(filter) != 3:
                        raise ValueError(
                            f"The given filter has incorrect intput type, {filter}. "
                            "Provide (key, predicate, value) tuples."
                        )
                    filter_k, filter_predicate, filter_val = filter
                    options_dict["filter_keys"].append(filter_k)
                    options_dict["filter_predicates"].append(filter_predicate)
                    options_dict["filter_values"].append(filter_val)
                continue

            option_val = getattr(options, field.name)
            if option_val:
                options_dict[field.name] = option_val

        return options_dict

    def _make_http_get_request(
        self,
        endpoint: str,
        params: Dict,
        timeout: float,
        _explain: bool = False,
    ):
        with warnings_on_slow_request(
            address=self._address, endpoint=endpoint, timeout=timeout, explain=_explain
        ):
            # Send a request.
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
                    err_str += (
                        f"Response(url={response.url},status={response.status_code})"
                    )
                raise RayStateApiException(err_str) from e

        # Process the response.
        response = response.json()
        if response["result"] is False:
            raise RayStateApiException(
                "API server internal error. See dashboard.log file for more details. "
                f"Error: {response['msg']}"
            )

        # Dictionary of `ListApiResponse` or `SummaryApiResponse`
        return response["data"]["result"]

    def get(
        self,
        resource: StateResource,
        id: str,
        options: Optional[GetApiOptions],
        _explain: bool = False,
    ) -> Optional[
        Union[
            ActorState,
            PlacementGroupState,
            NodeState,
            WorkerState,
            TaskState,
            List[ObjectState],
        ]
    ]:
        """Get resources states by id

        Args:
            resource_name: Resource names, i.e. 'workers', 'actors', 'nodes',
                'placement_groups', 'tasks', 'objects'.
                'jobs' and 'runtime-envs' are not supported yet.
            id: ID for the resource, i.e. 'node_id' for nodes.
            options: Get options. See `GetApiOptions` for details.
            _explain: Print the API information such as API
                latency or failed query information.

        Returns:
            None if not found, and found:
            - ActorState for actors
            - PlacementGroupState for placement groups
            - NodeState for nodes
            - WorkerState for workers
            - TaskState for tasks

            Empty list for objects if not found, or list of ObjectState for objects

        Raises:
            This doesn't catch any exceptions raised when the underlying request
            call raises exceptions. For example, it could raise `requests.Timeout`
            when timeout occurs.

            ValueError:
                if the resource could not be GET by id, i.e. jobs and runtime-envs.

        """
        # TODO(rickyyx): Make GET not using filters on list operation
        params = self._make_param(options)

        RESOURCE_ID_KEY_NAME = {
            StateResource.NODES: "node_id",
            StateResource.ACTORS: "actor_id",
            StateResource.PLACEMENT_GROUPS: "placement_group_id",
            StateResource.WORKERS: "worker_id",
            StateResource.TASKS: "task_id",
            StateResource.OBJECTS: "object_id",
        }
        if resource not in RESOURCE_ID_KEY_NAME:
            raise ValueError(f"Can't get {resource.name} by id.")

        params["filter_keys"] = [RESOURCE_ID_KEY_NAME[resource]]
        params["filter_predicates"] = ["="]
        params["filter_values"] = [id]
        params["detail"] = True
        endpoint = f"/api/v0/{resource.value}"

        list_api_response = self._make_http_get_request(
            endpoint=endpoint,
            params=params,
            timeout=options.timeout,
            _explain=_explain,
        )
        result = list_api_response["result"]

        # Empty result
        if len(result) == 0:
            return None

        if resource == StateResource.OBJECTS:
            # NOTE(rickyyx):
            # There might be multiple object entries for a single object id
            # because a single object could be referenced at different places
            # e.g. pinned as local variable, used as parameter
            return result

        # For the rest of the resources, there should only be a single entry
        # for a particular id.
        assert len(result) == 1
        return result[0]

    def _print_api_warning(self, resource: StateResource, api_response: dict):
        """Print the API warnings.

        We print warnings for users:
            1. when some data sources are not available
            2. when results were truncated at the data source
            3. when results were limited
            4. when callsites not enabled for listing objects

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            api_response: The dictionarified `ListApiResponse` or `SummaryApiResponse`.
        """
        # Print warnings if anything was given.
        warning_msgs = api_response.get("partial_failure_warning", None)
        if warning_msgs:
            warnings.warn(warning_msgs)

        # Print warnings if data is truncated at the data source.
        num_after_truncation = api_response["num_after_truncation"]
        total = api_response["total"]
        if total > num_after_truncation:
            # NOTE(rickyyx): For now, there's not much users could do (neither can we),
            # with hard truncation. Unless we allow users to set a higher
            # `RAY_MAX_LIMIT_FROM_DATA_SOURCE`, the data will always be truncated at the
            # data source.
            warnings.warn(
                (
                    f"{num_after_truncation} ({total} total) {resource.value} "
                    "are retrieved from the data source. "
                    f"{total - num_after_truncation} entries have been truncated. "
                    f"Max of {num_after_truncation} entries are retrieved from data "
                    "source to prevent over-sized payloads."
                ),
            )

        # Print warnings if return data is limited at the API server due to
        # limit enforced at the server side
        num_filtered = api_response["num_filtered"]
        data = api_response["result"]
        if num_filtered > len(data):
            warnings.warn(
                (
                    f"{len(data)}/{num_filtered} {resource.value} returned. "
                    "Use `--filter` to reduce the amount of data to return or "
                    "setting a higher limit with `--limit` to see all data. "
                ),
            )

        # Print the additional warnings.
        warnings_to_print = api_response.get("warnings", [])
        if warnings_to_print:
            for warning_to_print in warnings_to_print:
                warnings.warn(warning_to_print)

    def _raise_on_missing_output(self, resource: StateResource, api_response: dict):
        """Raise an exception when the API resopnse contains a missing output.

        Output can be missing if (1) Failures on some of data source queries (e.g.,
        `ray list tasks` queries all raylets, and if some of quries fail, it will
        contain missing output. If all quries fail, it will just fail). (2) Data
        is truncated because the output is too large.

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            api_response: The dictionarified `ListApiResponse` or `SummaryApiResponse`.
        """
        warning_msgs = api_response.get("partial_failure_warning", None)
        # TODO(sang) raise an exception on truncation after
        # https://github.com/ray-project/ray/pull/26801.
        if warning_msgs:
            raise RayStateApiException(
                f"Failed to retrieve all {resource.value} from the cluster. "
                f"It can happen when some of {resource.value} information is not "
                "reachable or the returned data is truncated because it is too large. "
                "To allow having missing output, set `raise_on_missing_output=False`. "
            )

    def list(
        self,
        resource: StateResource,
        options: ListApiOptions,
        raise_on_missing_output: bool,
        _explain: bool = False,
    ) -> List[Dict]:
        """List resources states

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            options: List options. See `ListApiOptions` for details.
            raise_on_missing_output: When True, raise an exception if the output
                is incomplete. Output can be incomplete if
                (1) there's a partial network failure when the source is distributed.
                (2) data is truncated because it is too large.
                Set it to False to avoid throwing an exception on missing data.
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
        params = self._make_param(options)
        list_api_response = self._make_http_get_request(
            endpoint=endpoint,
            params=params,
            timeout=options.timeout,
            _explain=_explain,
        )
        if raise_on_missing_output:
            self._raise_on_missing_output(resource, list_api_response)
        if _explain:
            self._print_api_warning(resource, list_api_response)
        return list_api_response["result"]

    def summary(
        self,
        resource: SummaryResource,
        *,
        options: SummaryApiOptions,
        raise_on_missing_output: bool,
        _explain: bool = False,
    ) -> Dict:
        """Summarize resources states

        Args:
            resource_name: Resource names,
                see `SummaryResource` for details.
            options: summary options. See `SummaryApiOptions` for details.
            raise_on_missing_output: Raise an exception if the output has missing data.
                Output can have missing data if (1) there's a partial network failure
                when the source is distributed. (2) data is truncated
                because it is too large.
            _explain: Print the API information such as API
                latency or failed query information.

        Returns:
            A dictionary of queried result from `SummaryApiResponse`.

        Raises:
            This doesn't catch any exceptions raised when the underlying request
            call raises exceptions. For example, it could raise `requests.Timeout`
            when timeout occurs.
        """
        params = {"timeout": options.timeout}
        endpoint = f"/api/v0/{resource.value}/summarize"
        summary_api_response = self._make_http_get_request(
            endpoint=endpoint,
            params=params,
            timeout=options.timeout,
            _explain=_explain,
        )
        if raise_on_missing_output:
            self._raise_on_missing_output(resource, summary_api_response)
        # TODO(sang): Add warning after
        # # https://github.com/ray-project/ray/pull/26801 is merged.
        return summary_api_response["result"]["node_id_to_summary"]


"""
Convenient Methods for get_<RESOURCE> by id
"""


def get_actor(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[ActorState]:
    return StateApiClient(address=address).get(
        StateResource.ACTORS, id, GetApiOptions(timeout=timeout), _explain=_explain
    )


# TODO(rickyyx:alpha-obs)
def get_job(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[JobState]:
    raise NotImplementedError("Get Job by id is currently not supported")


def get_placement_group(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[PlacementGroupState]:
    return StateApiClient(address=address).get(
        StateResource.PLACEMENT_GROUPS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


def get_node(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[NodeState]:
    return StateApiClient(address=address).get(
        StateResource.NODES,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


def get_worker(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[WorkerState]:
    return StateApiClient(address=address).get(
        StateResource.WORKERS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


def get_task(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[TaskState]:
    return StateApiClient(address=address).get(
        StateResource.TASKS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


def get_objects(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> List[ObjectState]:
    return StateApiClient(address=address).get(
        StateResource.OBJECTS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


"""
Convenient methods for list_<RESOURCE>

Supported arguments to the below methods, see `ListApiOptions`:
    address: The IP address and port of the head node. Defaults to
        http://localhost:8265.
    filters: Optional list of filter key-value pair.
    timeout: Time for the request.
    limit: Limit of entries in the result
    detail: If True, APIs will return more detailed output.
        In this case, it can query more sources (more expensive).
"""


def list_actors(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.ACTORS,
        options=ListApiOptions(
            limit=limit,
            timeout=timeout,
            filters=filters,
            detail=detail,
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_placement_groups(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.PLACEMENT_GROUPS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_nodes(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.NODES,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_jobs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.JOBS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_workers(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.WORKERS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_tasks(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.TASKS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_objects(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.OBJECTS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def list_runtime_envs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).list(
        StateResource.RUNTIME_ENVS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
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
    tail: int = DEFAULT_LOG_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
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
        timeout=timeout,
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
    timeout: int = DEFAULT_RPC_TIMEOUT,
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
    options_dict["timeout"] = timeout

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
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).summary(
        SummaryResource.TASKS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def summarize_actors(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).summary(
        SummaryResource.ACTORS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


def summarize_objects(
    address: str = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
):
    return StateApiClient(address=address).summary(
        SummaryResource.OBJECTS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )
