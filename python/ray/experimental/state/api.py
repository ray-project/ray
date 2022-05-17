import requests

from dataclasses import fields

import ray
from ray.experimental.state.common import (
    ListApiOptions,
    DEFAULT_RPC_TIMEOUT,
    DEFAULT_LIMIT,
)


def _get_api_server_url():
    assert ray.is_initialized()
    return f"http://{ray.worker.global_worker.node.address_info['webui_url']}"


# TODO(sang): Replace it with auto-generated methods.
def _list(resource_name: str, options: ListApiOptions, api_server_url: str = None):
    """Query the API server in address to list "resource_name" states.

    Args:
        resource_name: The name of the resource. E.g., actor, task.
        options: The options for the REST API that are translated to query strings.
        address: The address of API server. If it is not give, it assumes the ray
            is already connected and obtains the API server address using
            Ray API.
    """
    if api_server_url is None:
        api_server_url = _get_api_server_url()

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
    if not response["result"]:
        raise ValueError(
            "API server internal error. See dashboard.log file for more details."
        )
    return response["data"]["result"]


"""
Ray list API
"""


def list_actors(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "actors",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_placement_groups(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "placement_groups",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_nodes(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "nodes",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_jobs(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "jobs",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_workers(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "workers",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_tasks(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "tasks",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_objects(
    api_server_url: str = None,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_RPC_TIMEOUT,
):
    return _list(
        "objects",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


def list_runtime_envs(api_server_url: str = None, limit: int = 1000, timeout: int = 30):
    return _list(
        "runtime_envs",
        ListApiOptions(limit=limit, timeout=timeout),
        api_server_url=api_server_url,
    )


"""
Ray Resource API
"""


def summary_resources(
    per_node: bool = False,
    detail: bool = False,
    api_server_url: str = None,
    timeout: int = 30,
):
    if api_server_url is None:
        api_server_url = _get_api_server_url()

    # There are 4 APIs.
    # /api/v0/resources/ -> Cluster resource information.
    # /api/v0/resources/per_node -> Per node resource information.
    # /api/v0/resources/detail -> Cluster resource information in detail..
    # /api/v0/resources/per_node/detail -> Per node resource information in detail.
    url = f"{api_server_url}/api/v0/resources"
    if per_node:
        url += "/per_node"
    if detail:
        url += "/detail"

    r = requests.request(
        "GET",
        url,
        headers={"Content-Type": "application/json"},
        json=None,
        timeout=timeout,
    )
    r.raise_for_status()

    response = r.json()
    if not response["result"]:
        raise ValueError(
            "API server internal error. See dashboard.log file for more details."
        )
    return response["data"]["result"]
