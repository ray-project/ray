import urllib
import warnings
from dataclasses import fields
from typing import Dict, Generator, List, Optional, Tuple, Union

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
        resource: StateResource,
        _explain: bool = False,
    ):
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
        if response["result"] is False:
            raise RayStateApiException(
                "API server internal error. See dashboard.log file for more details. "
                f"Error: {response['msg']}"
            )

        # Dictionary of `ListApiResponse`
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
            resource=resource,
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

    def _print_list_api_warning(self, resource: StateResource, list_api_response: dict):
        """Print the API warnings.

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            list_api_response: The dictionarified `ListApiResponse`.
        """
        # Print warnings if anything was given.
        warning_msgs = list_api_response.get("partial_failure_warning", None)
        if warning_msgs:
            warnings.warn(warning_msgs)

        # Print warnings if data is truncated.
        data = list_api_response["result"]
        total = list_api_response["total"]
        if total > len(data):
            warnings.warn(
                (
                    f"{len(data)} ({total} total) {resource.value} "
                    f"are returned. {total - len(data)} entries have been truncated. "
                    "Use `--filter` to reduce the amount of data to return "
                    "or increase the limit by specifying`--limit`."
                ),
            )

    def list(
        self, resource: StateResource, options: ListApiOptions, _explain: bool = False
    ) -> Union[Dict, List]:
        """List resources states

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
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
        params = self._make_param(options)
        list_api_response = self._make_http_get_request(
            endpoint=endpoint,
            params=params,
            timeout=options.timeout,
            resource=resource,
            _explain=_explain,
        )
        if _explain:
            self._print_list_api_warning(resource, list_api_response)
        return list_api_response["result"]

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
            A dictionary of queried result from `SummaryApiResponse`,

        Raises:
            This doesn't catch any exceptions raised when the underlying request
            call raises exceptions. For example, it could raise `requests.Timeout`
            when timeout occurs.
        """
        params = {"timeout": options.timeout}
        endpoint = f"/api/v0/{resource.value}/summarize"
        list_api_response = self._make_http_get_request(
            endpoint=endpoint,
            params=params,
            timeout=options.timeout,
            resource=resource,
            _explain=_explain,
        )
        result = list_api_response["result"]
        return result["node_id_to_summary"]


"""
Convenient Methods for get_<RESOURCE> by id
"""


def get_actor(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[ActorState]:
    return StateApiClient(api_server_address=address).get(
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
    return StateApiClient(api_server_address=address).get(
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
    return StateApiClient(api_server_address=address).get(
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
    return StateApiClient(api_server_address=address).get(
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
    return StateApiClient(api_server_address=address).get(
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
    return StateApiClient(api_server_address=address).get(
        StateResource.OBJECTS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


"""
Convenient methods for list_<RESOURCE>

Supported arguments to the below methods, see `ListApiOptions`:
    address: The address of the Ray state server. If None, it assumes a running Ray
        deployment exists and will query the GCS for auto-configuration.
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
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.ACTORS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_placement_groups(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.PLACEMENT_GROUPS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_nodes(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.NODES,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_jobs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.JOBS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_workers(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.WORKERS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_tasks(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.TASKS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_objects(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.OBJECTS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        _explain=_explain,
    )


def list_runtime_envs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    _explain: bool = False,
):
    return StateApiClient(api_server_address=address).list(
        StateResource.RUNTIME_ENVS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
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
