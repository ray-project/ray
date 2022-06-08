import warnings

from dataclasses import fields
from typing import Dict, List, Optional, Union

import ray

from ray.experimental.state.common import ListApiOptions, StateResource
from ray.experimental.state.exception import RayStateApiException
from ray.dashboard.modules.dashboard_sdk import SubmissionClient


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
            and ray.worker.global_worker.node.address_info["webui_url"] is not None
        )
        return f"http://{ray.worker.global_worker.node.address_info['webui_url']}"

    @classmethod
    def _get_query_string(cls, options: ListApiOptions) -> str:
        query_strings = []
        for field in fields(options):
            query_strings.append(f"{field.name}={getattr(options, field.name)}")
        return "&".join(query_strings)

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
        # Append the ListApiOptions to the url
        endpoint = f"/api/v0/{resource.value}?{self._get_query_string(options)}"

        # NOTE(rickyyx)
        # This might raise `requests.Timeout` exceptions and other related requests
        # exceptions. Users of the functions are expected to handle this.
        response = self._do_request(
            "GET",
            endpoint,
            timeout=options.timeout,
        )

        if response.status_code != 200:
            self._raise_error(response)

        response = response.json()
        if response["result"] is False:
            raise RayStateApiException(
                "API server internal error. See dashboard.log file for more details. "
                f"Error: {response['msg']}"
            )

        # Print warnings if anything was given.
        warnings_msgs = response["data"].get("partial_failure_warning", None)
        if warnings_msgs and _explain:
            warnings.warn(warnings_msgs, RuntimeWarning)

        return response["data"]["result"]


"""
Convenient methods for list_<RESOURCE>

Supported arguments to the below methods, see `ListApiOptions`:
    address: The address of the Ray state server. If None, it assumes a running Ray
        deployment exists and will query the GCS for auto-configuration.
    timeout: Time for the request.
    limit: Limit of entries in the result
"""


def list_actors(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.ACTORS, options=ListApiOptions(**kwargs), _explain=_explain
    )


def list_jobs(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.JOBS, options=ListApiOptions(**kwargs), _explain=_explain
    )


def list_placement_groups(
    address: Optional[str] = None, _explain: bool = False, **kwargs
):
    return StateApiClient(api_server_address=address).list(
        StateResource.PLACEMENT_GROUPS,
        options=ListApiOptions(**kwargs),
        _explain=_explain,
    )


def list_nodes(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.NODES,
        options=ListApiOptions(**kwargs),
        _explain=_explain,
    )


def list_workers(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.WORKERS,
        options=ListApiOptions(**kwargs),
        _explain=_explain,
    )


def list_tasks(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.TASKS, options=ListApiOptions(**kwargs), _explain=_explain
    )


def list_objects(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.OBJECTS,
        options=ListApiOptions(**kwargs),
        _explain=_explain,
    )


def list_runtime_envs(address: Optional[str] = None, _explain: bool = False, **kwargs):
    return StateApiClient(api_server_address=address).list(
        StateResource.RUNTIME_ENVS,
        options=ListApiOptions(**kwargs),
        _explain=_explain,
    )
