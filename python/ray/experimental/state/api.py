import requests
import warnings

from typing import List, Tuple

import ray
from ray.experimental.state.common import (
    SupportedFilterType,
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
    r = requests.request(
        "GET",
        f"{api_server_url}/api/v0/{resource_name}",
        params=params,
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
        if warning_msg:
            warnings.warn(warning_msg, RuntimeWarning)

    return r.json()["data"]["result"]


def list_actors(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "actors",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_placement_groups(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "placement_groups",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_nodes(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "nodes",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_jobs(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "jobs",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_workers(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "workers",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_tasks(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "tasks",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_objects(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "objects",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )


def list_runtime_envs(
    api_server_url: str = None,
    filters: List[Tuple[str, SupportedFilterType]] = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
    _explain: bool = False,
):
    return _list(
        "runtime_envs",
        ListApiOptions(limit=limit, timeout=timeout, filters=filters),
        api_server_url=api_server_url,
        _explain=_explain,
    )
