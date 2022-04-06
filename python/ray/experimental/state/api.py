import requests

from dataclasses import fields

import ray
from ray.experimental.state.common import ListApiOptions


def _list(resource_name: str, options, address: str = None):
    if address is None:
        assert ray.is_initialized()
        address = f"http://{ray.worker.global_worker.node.address_info['webui_url']}"

    kvs = []
    for field in fields(options):
        kvs.append(f"{field.name}={getattr(options, field.name)}")
    r = requests.request(
        "GET",
        f"{address}/api/v0/{resource_name}?{'?'.join(kvs)}",
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
    return r.json()["data"]["result"]


# TODO(sang): Replace it with auto-generated methods.
def list_actors(address: str = None, limit: int = 1000, timeout: int = 30):
    return _list(
        "actors", ListApiOptions(limit=limit, timeout=timeout), address=address
    )


def list_placement_groups(address: str = None, limit: int = 1000, timeout: int = 30):
    return _list(
        "placement_groups",
        ListApiOptions(limit=limit, timeout=timeout),
        address=address,
    )


def list_nodes(address: str = None, limit: int = 1000, timeout: int = 30):
    return _list("nodes", ListApiOptions(limit=limit, timeout=timeout), address=address)


def list_jobs(address: str = None, limit: int = 1000, timeout: int = 30):
    return _list("jobs", ListApiOptions(limit=limit, timeout=timeout), address=address)


def list_workers(address: str = None, limit: int = 1000, timeout: int = 30):
    return _list(
        "workers", ListApiOptions(limit=limit, timeout=timeout), address=address
    )
