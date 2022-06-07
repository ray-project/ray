from typing import Any, List, Optional, Dict
from venv import create
import requests
import warnings

from dataclasses import fields, replace
from ray.dashboard.modules.dashboard_sdk import SubmissionClient

import ray
from ray.experimental.state.common import (
    ListApiOptions,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_LIMIT,
)
from ray.experimental.state.exception import RayStateApiException


class StateApiClient(SubmissionClient):
    def __init__(
        self,
        options: ListApiOptions,
        api_server_url: str = None,
    ):
        super().__init__(
            self._get_default_api_server_address()
            if api_server_url is None
            else api_server_url,
            create_cluster_if_needed=False,
            headers={"Content-Type": "application/json"},
        )
        self._default_options = options

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

    def _do_list(
        self, resource_name: str, _explain: bool, **override_options
    ) -> List[Any]:
        # Override the default ListApiOptions if there are any command specific options
        options = replace(self._default_options, **override_options)

        # Append the ListApiOptions to the url
        endpoint = f"/api/v0/{resource_name}?{self._get_query_string(options)}"
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

        if _explain:
            # Print warnings if anything was given.
            warning_msg = response["data"].get("partial_failure_warning", None)
            if warning_msg is not None:
                warnings.warn(warning_msg, RuntimeWarning)

        return response["data"]["result"]

    def list_actors(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("actors", _explain, **override_options)

    def list_placement_groups(
        self, _explain: bool = False, **override_options
    ) -> List[Any]:
        return self._do_list("placement_groups", _explain, **override_options)

    def list_nodes(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("nodes", _explain, **override_options)

    def list_jobs(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("jobs", _explain, **override_options)

    def list_workers(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("workers", _explain, **override_options)

    def list_tasks(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("tasks", _explain, **override_options)

    def list_objects(self, _explain: bool = False, **override_options) -> List[Any]:
        return self._do_list("objects", _explain, **override_options)

    def list_runtime_envs(
        self, _explain: bool = False, **override_options
    ) -> List[Any]:
        return self._do_list("runtime_envs", _explain, **override_options)


# TODO(sang): Replace it with auto-generated methods.
def _list(
    resource_name: str,
    options: ListApiOptions,
    api_server_url: str = None,
    _explain: bool = False,
):
    """Query the API server in address to list "resource_name" states.

    Args:
        resource_name: The name of the resource. E.g., actor, task.
        options: The options for the REST API that are translated to query strings.
        api_server_url: The address of API server. If it is not give, it assumes the ray
            is already connected and obtains the API server address using
            Ray API.
        explain: Print the API information such as API
            latency or failed query information.
    """
    if api_server_url is None:
        assert ray.is_initialized()
        api_server_url = (
            f"http://{ray.worker.global_worker.node.address_info['webui_url']}"
        )

    query_strings = []
    for field in fields(options):
        query_strings.append(f"{field.name}={getattr(options, field.name)}")
    r = requests.request(
        "GET",
        f"{api_server_url}/api/v0/{resource_name}?{'&'.join(query_strings)}",
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=options.timeout,
    )
    r.raise_for_status()

    response = r.json()
    if response["result"] is False:
        raise RayStateApiException(
            "API server internal error. See dashboard.log file for more details. "
            f"Error: {response['msg']}"
        )

    if _explain:
        # Print warnings if anything was given.
        warning_msg = response["data"].get("partial_failure_warning", None)
        if warning_msg is not None:
            warnings.warn(warning_msg, RuntimeWarning)

    return r.json()["data"]["result"]


def list_actors(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "actors",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_placement_groups(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "placement_groups",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_nodes(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "nodes",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_jobs(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "jobs",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_workers(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "workers",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_tasks(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "tasks",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_objects(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "objects",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_runtime_envs(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "runtime_envs",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
        _explain=_explain,
    )
