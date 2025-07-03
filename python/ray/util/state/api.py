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
from ray.dashboard.utils import (
    get_address_for_submission_client,
    ray_address_to_api_server_url,
)
from ray.util.annotations import DeveloperAPI
from ray.util.state.common import (
    DEFAULT_LIMIT,
    DEFAULT_RPC_TIMEOUT,
    ActorState,
    ClusterEventState,
    GetApiOptions,
    GetLogOptions,
    JobState,
    ListApiOptions,
    NodeState,
    ObjectState,
    PlacementGroupState,
    PredicateType,
    RuntimeEnvState,
    StateResource,
    SummaryApiOptions,
    SummaryResource,
    SupportedFilterType,
    TaskState,
    WorkerState,
    dict_to_state,
)
from ray.util.state.exception import RayStateApiException, ServerUnavailable

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

Usage:
    1. [Recommended] With StateApiClient:
    ```
        client = StateApiClient(address="auto")
        data = client.list(StateResource.NODES)
        ...
    ```

    2. With SDK APIs:
    The API creates a `StateApiClient` for each invocation. So if multiple
    invocations of listing are used, it is better to reuse the `StateApiClient`
    as suggested above.
    ```
        data = list_nodes(address="auto")
    ```
"""


@DeveloperAPI
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
            address: Ray bootstrap address (e.g. `127.0.0.0:6379`, `auto`), or Ray
                Client address (e.g. `ray://<head-node-ip>:10001`), or Ray dashboard
                address (e.g. `http://<head-node-ip>:8265`).
                If not provided, it will be detected automatically from any running
                local Ray cluster.
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

        # Resolve API server URL
        api_server_url = get_address_for_submission_client(address)

        super().__init__(
            address=api_server_url,
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
                            f"The given filter has incorrect input type, {filter}. "
                            "Provide (key, predicate, value) tuples."
                        )
                    filter_k, filter_predicate, filter_val = filter
                    options_dict["filter_keys"].append(filter_k)
                    options_dict["filter_predicates"].append(filter_predicate)
                    options_dict["filter_values"].append(filter_val)
                continue

            option_val = getattr(options, field.name)
            if option_val is not None:
                options_dict[field.name] = option_val

        return options_dict

    def _make_http_get_request(
        self,
        endpoint: str,
        params: Dict,
        timeout: float,
        _explain: bool = False,
    ) -> Dict:
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
                # If we have a valid JSON error, don't raise a generic exception but
                # instead let the caller parse it to raise a more precise exception.
                if (
                    response.status_code == 500
                    and "application/json"
                    not in response.headers.get("Content-Type", "")
                ):
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                err_str = f"Failed to make request to {self._address}{endpoint}. "

                # Best-effort to give hints to users on potential reasons of connection
                # failure.
                err_str += (
                    "Failed to connect to API server. Please check the API server "
                    "log for details. Make sure dependencies are installed with "
                    "`pip install ray[default]`. Please also check dashboard is "
                    "available, and included when starting ray cluster, "
                    "i.e. `ray start --include-dashboard=True --head`. "
                )
                if response is None:
                    raise ServerUnavailable(err_str)

                err_str += f"Response(url={response.url},status={response.status_code})"
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
            JobState,
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
            None if not found, and if found:
            - ActorState for actors
            - PlacementGroupState for placement groups
            - NodeState for nodes
            - WorkerState for workers
            - TaskState for tasks
            - JobState for jobs

            Empty list for objects if not found, or list of ObjectState for objects

        Raises:
            Exception: This doesn't catch any exceptions raised when the underlying request
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
            StateResource.JOBS: "submission_id",
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

        result = [dict_to_state(d, resource) for d in result]
        if resource == StateResource.OBJECTS:
            # NOTE(rickyyx):
            # There might be multiple object entries for a single object id
            # because a single object could be referenced at different places
            # e.g. pinned as local variable, used as parameter
            return result

        if resource == StateResource.TASKS:
            # There might be multiple task attempts given a task id due to
            # task retries.
            if len(result) == 1:
                return result[0]
            return result

        # For the rest of the resources, there should only be a single entry
        # for a particular id.
        assert len(result) == 1
        return result[0]

    def _print_api_warning(
        self,
        resource: StateResource,
        api_response: dict,
        warn_data_source_not_available: bool = True,
        warn_data_truncation: bool = True,
        warn_limit: bool = True,
        warn_server_side_warnings: bool = True,
    ):
        """Print the API warnings.

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            api_response: The dictionarified `ListApiResponse` or `SummaryApiResponse`.
            warn_data_source_not_available: Warn when some data sources
                are not available.
            warn_data_truncation: Warn when results were truncated at
                the data source.
            warn_limit: Warn when results were limited.
            warn_server_side_warnings: Warn when the server side generates warnings
                (E.g., when callsites not enabled for listing objects)
        """
        # Print warnings if anything was given.
        if warn_data_source_not_available:
            warning_msgs = api_response.get("partial_failure_warning", None)
            if warning_msgs:
                warnings.warn(warning_msgs)

        if warn_data_truncation:
            # Print warnings if data is truncated at the data source.
            num_after_truncation = api_response["num_after_truncation"]
            total = api_response["total"]
            if total > num_after_truncation:
                # NOTE(rickyyx): For now, there's not much users
                # could do (neither can we), with hard truncation.
                # Unless we allow users to set a higher
                # `RAY_MAX_LIMIT_FROM_DATA_SOURCE`, the data will
                # always be truncated at the data source.
                warnings.warn(
                    (
                        "The returned data may contain incomplete result. "
                        f"{num_after_truncation} ({total} total from the cluster) "
                        f"{resource.value} are retrieved from the data source. "
                        f"{total - num_after_truncation} entries have been truncated. "
                        f"Max of {num_after_truncation} entries are retrieved "
                        "from data source to prevent over-sized payloads."
                    ),
                )

        if warn_limit:
            # Print warnings if return data is limited at the API server due to
            # limit enforced at the server side
            num_filtered = api_response["num_filtered"]
            data = api_response["result"]
            if num_filtered > len(data):
                warnings.warn(
                    (
                        f"Limit last {len(data)} entries "
                        f"(Total {num_filtered}). Use `--filter` to reduce "
                        "the amount of data to return or "
                        "setting a higher limit with `--limit` to see all data. "
                    ),
                )

        if warn_server_side_warnings:
            # Print the additional warnings.
            warnings_to_print = api_response.get("warnings", [])
            if warnings_to_print:
                for warning_to_print in warnings_to_print:
                    warnings.warn(warning_to_print)

    def _raise_on_missing_output(self, resource: StateResource, api_response: dict):
        """Raise an exception when the API resopnse contains a missing output.

        Output can be missing if (1) Failures on some of data source queries (e.g.,
        `ray list tasks` queries all raylets, and if some of queries fail, it will
        contain missing output. If all queries fail, it will just fail). (2) Data
        is truncated because the output is too large.

        Args:
            resource: Resource names, i.e. 'jobs', 'actors', 'nodes',
                see `StateResource` for details.
            api_response: The dictionarified `ListApiResponse` or `SummaryApiResponse`.
        """
        # Raise an exception if there are partial failures that cause missing output.
        warning_msgs = api_response.get("partial_failure_warning", None)
        if warning_msgs:
            raise RayStateApiException(
                f"Failed to retrieve all {resource.value} from the cluster because"
                "they are not reachable due to query failures to the data sources. "
                "To avoid raising an exception and allow having missing output, "
                "set `raise_on_missing_output=False`. "
            )
        # Raise an exception is there is data truncation that cause missing output.
        total = api_response["total"]
        num_after_truncation = api_response["num_after_truncation"]

        if total != num_after_truncation:
            raise RayStateApiException(
                f"Failed to retrieve all {total} {resource.value} from the cluster "
                "because they are not reachable due to data truncation. It happens "
                "when the returned data is too large "
                # When the data is truncated, the truncation
                # threshold == num_after_truncation. We cannot set this to env
                # var because the CLI side might not have the correct env var.
                f"(> {num_after_truncation}) "
                "To avoid raising an exception and allow having missing output, "
                "set `raise_on_missing_output=False`. "
            )

    def list(
        self,
        resource: StateResource,
        options: ListApiOptions,
        raise_on_missing_output: bool,
        _explain: bool = False,
    ) -> List[
        Union[
            ActorState,
            JobState,
            NodeState,
            TaskState,
            ObjectState,
            PlacementGroupState,
            RuntimeEnvState,
            WorkerState,
            ClusterEventState,
        ]
    ]:
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
            Exception: This doesn't catch any exceptions raised when the
                underlying request call raises exceptions. For example, it could
                raise `requests.Timeout` when timeout occurs.

        """
        if options.has_conflicting_filters():
            # return early with empty list when there are conflicting filters
            return []

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
        return [dict_to_state(d, resource) for d in list_api_response["result"]]

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
            Exception: This doesn't catch any exceptions raised when the
                underlying request call raises exceptions. For example, it could
                raise `requests.Timeout` when timeout occurs.
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
        if _explain:
            # There's no limit applied to summary, so we shouldn't warn.
            self._print_api_warning(resource, summary_api_response, warn_limit=False)
        return summary_api_response["result"]["node_id_to_summary"]


@DeveloperAPI
def get_actor(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[ActorState]:
    """Get an actor by id.

    Args:
        id: Id of the actor
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state API requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if actor not found, or
        :class:`ActorState <ray.util.state.common.ActorState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.ACTORS, id, GetApiOptions(timeout=timeout), _explain=_explain
    )


@DeveloperAPI
def get_job(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[JobState]:
    """Get a submission job detail by id.

    Args:
        id: Submission ID obtained from job API.
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state API requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if job not found, or
        :class:`JobState <ray.util.state.common.JobState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.JOBS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def get_placement_group(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[PlacementGroupState]:
    """Get a placement group by id.

    Args:
        id: Id of the placement group
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state APIs requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if actor not found, or
        :class:`~ray.util.state.common.PlacementGroupState`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.PLACEMENT_GROUPS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def get_node(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[NodeState]:
    """Get a node by id.

    Args:
        id: Id of the node.
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state APIs requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if actor not found, or
        :class:`NodeState <ray.util.state.common.NodeState>`.

    Raises:
        RayStateApiException: if the CLI is failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.NODES,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def get_worker(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[WorkerState]:
    """Get a worker by id.

    Args:
        id: Id of the worker
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state APIs requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if actor not found, or
        :class:`WorkerState <ray.util.state.common.WorkerState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.WORKERS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def get_task(
    id: Union[str, "ray.ObjectRef"],
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> Optional[TaskState]:
    """Get task attempts of a task by id.

    Args:
        id: String id of the task or ObjectRef that corresponds to task
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state APIs requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        None if task not found, or a list of
        :class:`~ray.util.state.common.TaskState`
        from the task attempts.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    str_id: str
    if isinstance(id, str):
        str_id = id
    else:
        str_id = id.task_id().hex()
    return StateApiClient(address=address).get(
        StateResource.TASKS,
        str_id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def get_objects(
    id: str,
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
) -> List[ObjectState]:
    """Get objects by id.

    There could be more than 1 entry returned since an object could be
    referenced at different places.

    Args:
        id: Id of the object
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout value for the state APIs requests made.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`~ray.util.state.common.ObjectState`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).get(
        StateResource.OBJECTS,
        id,
        GetApiOptions(timeout=timeout),
        _explain=_explain,
    )


@DeveloperAPI
def list_actors(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[ActorState]:
    """List actors in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("id", "=", "abcd")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `ActorState`)
            will be queried and returned. See
            :class:`ActorState <ray.util.state.common.ActorState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`ActorState <ray.util.state.common.ActorState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
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


@DeveloperAPI
def list_placement_groups(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[PlacementGroupState]:
    """List placement groups in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("state", "=", "abcd")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `PlacementGroupState`)
            will be queried and returned. See
            :class:`~ray.util.state.common.PlacementGroupState`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of :class:`~ray.util.state.common.PlacementGroupState`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.PLACEMENT_GROUPS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_nodes(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[NodeState]:
    """List nodes in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("node_name", "=", "abcd")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `NodeState`)
            will be queried and returned. See
            :class:`NodeState <ray.util.state.common.NodeState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of dictionarified
        :class:`NodeState <ray.util.state.common.NodeState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.NODES,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_jobs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[JobState]:
    """List jobs submitted to the cluster by :ref:`ray job submission <jobs-overview>`.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("status", "=", "abcd")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `JobState`)
            will be queried and returned. See
            :class:`JobState <ray.util.state.common.JobState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of dictionarified
        :class:`JobState <ray.util.state.common.JobState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.JOBS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_workers(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[WorkerState]:
    """List workers in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("is_alive", "=", "True")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `WorkerState`)
            will be queried and returned. See
            :class:`WorkerState <ray.util.state.common.WorkerState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`WorkerState <ray.util.state.common.WorkerState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.WORKERS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_tasks(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[TaskState]:
    """List tasks in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("is_alive", "=", "True")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `TaskState`)
            will be queried and returned. See
            :class:`TaskState <ray.util.state.common.TaskState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`TaskState <ray.util.state.common.TaskState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.TASKS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_objects(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[ObjectState]:
    """List objects in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("ip", "=", "0.0.0.0")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `ObjectState`)
            will be queried and returned. See
            :class:`ObjectState <ray.util.state.common.ObjectState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`ObjectState <ray.util.state.common.ObjectState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.OBJECTS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_runtime_envs(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[RuntimeEnvState]:
    """List runtime environments in the cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        filters: List of tuples of filter key, predicate (=, or !=), and
            the filter value. E.g., `("node_id", "=", "abcdef")`
            String filter values are case-insensitive.
        limit: Max number of entries returned by the state backend.
        timeout: Max timeout value for the state APIs requests made.
        detail: When True, more details info (specified in `RuntimeEnvState`)
            will be queried and returned. See
            :class:`RuntimeEnvState <ray.util.state.common.RuntimeEnvState>`.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Returns:
        List of
        :class:`RuntimeEnvState <ray.util.state.common.RuntimeEnvState>`.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).list(
        StateResource.RUNTIME_ENVS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def list_cluster_events(
    address: Optional[str] = None,
    filters: Optional[List[Tuple[str, PredicateType, SupportedFilterType]]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    detail: bool = False,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> List[Dict]:
    return StateApiClient(address=address).list(
        StateResource.CLUSTER_EVENTS,
        options=ListApiOptions(
            limit=limit, timeout=timeout, filters=filters, detail=detail
        ),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


"""
Log APIs
"""


@DeveloperAPI
def get_log(
    address: Optional[str] = None,
    node_id: Optional[str] = None,
    node_ip: Optional[str] = None,
    filename: Optional[str] = None,
    actor_id: Optional[str] = None,
    task_id: Optional[str] = None,
    pid: Optional[int] = None,
    follow: bool = False,
    tail: int = -1,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    suffix: str = "out",
    encoding: Optional[str] = "utf-8",
    errors: Optional[str] = "strict",
    submission_id: Optional[str] = None,
    attempt_number: int = 0,
    _interval: Optional[float] = None,
) -> Generator[str, None, None]:
    """Retrieve log file based on file name or some entities ids (pid, actor id, task id).

    Examples:
        .. testcode::
            :hide:

            import ray
            import time

            ray.shutdown()
            ray.init()

            # Wait for the node to be registered to the dashboard
            time.sleep(5)

        .. testcode::

            import ray
            from ray.util.state import get_log

            # Node id could be retrieved from list_nodes() or ray.nodes()
            node_id = ray.nodes()[0]["NodeID"]
            filename = "raylet.out"
            for l in get_log(filename=filename, node_id=node_id):
               print(l)

        .. testoutput::
            :options: +MOCK

            [2023-05-19 12:35:18,347 I 4259 68399276] (raylet) io_service_pool.cc:35: IOServicePool is running with 1 io_service.
            [2023-05-19 12:35:18,348 I 4259 68399276] (raylet) store_runner.cc:32: Allowing the Plasma store to use up to 2.14748GB of memory.
            [2023-05-19 12:35:18,348 I 4259 68399276] (raylet) store_runner.cc:48: Starting object store with directory /tmp, fallback /tmp/ray, and huge page support disabled

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If not specified, it will be retrieved from the initialized ray cluster.
        node_id: Id of the node containing the logs .
        node_ip: Ip of the node containing the logs. (At least one of the node_id and
            node_ip have to be supplied when identifying a node).
        filename: Name of the file (relative to the ray log directory) to be retrieved.
        actor_id: Id of the actor if getting logs from an actor.
        task_id: Id of the task if getting logs from a non concurrent actor.
            For concurrent actor, please query the log with actor_id.
        pid: PID of the worker if getting logs generated by a worker. When querying
            with pid, either node_id or node_ip must be supplied.
        follow: When set to True, logs will be streamed and followed.
        tail: Number of lines to get from the end of the log file. Set to -1 for getting
            the entire log.
        timeout: Max timeout for requests made when getting the logs.
        suffix: The suffix of the log file if query by id of tasks/workers/actors. Default to "out".
        encoding: The encoding used to decode the content of the log file. Default is
            "utf-8". Use None to get binary data directly.
        errors: The error handling scheme to use for decoding errors. Default is
            "strict". See https://docs.python.org/3/library/codecs.html#error-handlers
        submission_id: Job submission ID if getting log from a submission job.
        attempt_number: The attempt number of the task if getting logs generated by a task.
        _interval: The interval in secs to print new logs when `follow=True`.

    Return:
        A Generator of log line, None for SendType and ReturnType.

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501

    api_server_url = ray_address_to_api_server_url(address)
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
        suffix=suffix,
        submission_id=submission_id,
        attempt_number=attempt_number,
    )
    options_dict = {}
    for field in fields(options):
        option_val = getattr(options, field.name)
        if option_val is not None:
            options_dict[field.name] = option_val

    with requests.get(
        f"{api_server_url}/api/v0/logs/{media_type}?"
        f"{urllib.parse.urlencode(options_dict)}",
        stream=True,
    ) as r:
        if r.status_code != 200:
            raise RayStateApiException(r.text)
        for chunk in r.iter_content(chunk_size=None):
            if encoding is not None:
                chunk = chunk.decode(encoding=encoding, errors=errors)
            yield chunk


@DeveloperAPI
def list_logs(
    address: Optional[str] = None,
    node_id: Optional[str] = None,
    node_ip: Optional[str] = None,
    glob_filter: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
) -> Dict[str, List[str]]:
    """Listing log files available.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If not specified, it will be retrieved from the initialized ray cluster.
        node_id: Id of the node containing the logs.
        node_ip: Ip of the node containing the logs.
        glob_filter: Name of the file (relative to the ray log directory) to be
            retrieved. E.g. `glob_filter="*worker*"` for all worker logs.
        actor_id: Id of the actor if getting logs from an actor.
        timeout: Max timeout for requests made when getting the logs.
        _interval: The interval in secs to print new logs when `follow=True`.

    Return:
        A dictionary where the keys are log groups (e.g. gcs, raylet, worker), and
        values are list of log filenames.

    Raises:
        RayStateApiException: if the CLI failed to query the data, or ConnectionError if
            failed to resolve the ray address.
    """  # noqa: E501
    assert (
        node_ip is not None or node_id is not None
    ), "At least one of node ip and node id is required"

    api_server_url = ray_address_to_api_server_url(address)

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
    # TODO(rickyx): we could do better at error handling here.
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


@DeveloperAPI
def summarize_tasks(
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> Dict:
    """Summarize the tasks in cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout for requests made when getting the states.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Return:
        Dictionarified
        :class:`~ray.util.state.common.TaskSummaries`

    Raises:
        RayStateApiException: if the CLI is failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).summary(
        SummaryResource.TASKS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def summarize_actors(
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> Dict:
    """Summarize the actors in cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout for requests made when getting the states.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Return:
        Dictionarified
        :class:`~ray.util.state.common.ActorSummaries`

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).summary(
        SummaryResource.ACTORS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )


@DeveloperAPI
def summarize_objects(
    address: Optional[str] = None,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    raise_on_missing_output: bool = True,
    _explain: bool = False,
) -> Dict:
    """Summarize the objects in cluster.

    Args:
        address: Ray bootstrap address, could be `auto`, `localhost:6379`.
            If None, it will be resolved automatically from an initialized ray.
        timeout: Max timeout for requests made when getting the states.
        raise_on_missing_output: When True, exceptions will be raised if
            there is missing data due to truncation/data source unavailable.
        _explain: Print the API information such as API latency or
            failed query information.

    Return:
        Dictionarified :class:`~ray.util.state.common.ObjectSummaries`

    Raises:
        RayStateApiException: if the CLI failed to query the data.
    """  # noqa: E501
    return StateApiClient(address=address).summary(
        SummaryResource.OBJECTS,
        options=SummaryApiOptions(timeout=timeout),
        raise_on_missing_output=raise_on_missing_output,
        _explain=_explain,
    )
