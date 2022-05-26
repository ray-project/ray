import requests
import warnings

from dataclasses import fields

import ray
from ray.experimental.state.common import (
    ListApiOptions,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_LIMIT,
)
from ray.experimental.state.exception import RayStateApiException


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
