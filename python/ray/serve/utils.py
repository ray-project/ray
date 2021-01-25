import asyncio
from functools import singledispatch
import importlib
from itertools import groupby
import json
import logging
import random
import string
import time
from typing import Iterable, List, Dict, Tuple
import os
from ray.serve.exceptions import RayServeException
from collections import UserDict
from pathlib import Path

import starlette.requests
import requests
import numpy as np
import pydantic

import ray
from ray.serve.constants import HTTP_PROXY_TIMEOUT
from ray.ray_constants import MEMORY_RESOURCE_UNIT_BYTES

ACTOR_FAILURE_RETRY_TIMEOUT_S = 60


class ServeMultiDict(UserDict):
    """Compatible data structure to simulate Starlette Request query_args."""

    def getlist(self, key):
        """Return the list of items for a given key."""
        return self.data.get(key, [])


class ServeRequest:
    """The request object used when passing arguments via ServeHandle.

    ServeRequest partially implements the API of Starlette Request. You only
    need to write your model serving code once; it can be queried by both HTTP
    and Python.

    To use the full Starlette Request interface with ServeHandle, you may
    instead directly pass in a Starlette Request object to the ServeHandle.
    """

    def __init__(self, data, kwargs, headers, method):
        self._data = data
        self._kwargs = ServeMultiDict(kwargs)
        self._headers = headers
        self._method = method

    @property
    def headers(self):
        """The HTTP headers from ``handle.option(http_headers=...)``."""
        return self._headers

    @property
    def method(self):
        """The HTTP method data from ``handle.option(http_method=...)``."""
        return self._method

    @property
    def query_params(self):
        """The keyword arguments from ``handle.remote(**kwargs)``."""
        return self._kwargs

    async def json(self):
        """The request dictionary, from ``handle.remote(dict)``."""
        if not isinstance(self._data, dict):
            raise RayServeException("Request data is not a dictionary. "
                                    f"It is {type(self._data)}.")
        return self._data

    async def form(self):
        """The request dictionary, from ``handle.remote(dict)``."""
        if not isinstance(self._data, dict):
            raise RayServeException("Request data is not a dictionary. "
                                    f"It is {type(self._data)}.")
        return self._data

    async def body(self):
        """The request data from ``handle.remote(obj)``."""
        return self._data


def parse_request_item(request_item):
    arg = request_item.args[0] if len(request_item.args) == 1 else None

    # If the input data from handle is web request, we don't need to wrap
    # it in ServeRequest.
    if isinstance(arg, starlette.requests.Request):
        return arg

    return ServeRequest(
        arg,
        request_item.kwargs,
        headers=request_item.metadata.http_headers,
        method=request_item.metadata.http_method,
    )


def _get_logger():
    logger = logging.getLogger("ray.serve")
    # TODO(simon): Make logging level configurable.
    log_level = os.environ.get("SERVE_LOG_DEBUG")
    if log_level and int(log_level):
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


logger = _get_logger()


class ServeEncoder(json.JSONEncoder):
    """Ray.Serve's utility JSON encoder. Adds support for:
        - bytes
        - Pydantic types
        - Exceptions
        - numpy.ndarray
    """

    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, bytes):
            return o.decode("utf-8")
        if isinstance(o, pydantic.BaseModel):
            return o.dict()
        if isinstance(o, Exception):
            return str(o)
        if isinstance(o, np.ndarray):
            if o.dtype.kind == "f":  # floats
                o = o.astype(float)
            if o.dtype.kind in {"i", "u"}:  # signed and unsigned integers.
                o = o.astype(int)
            return o.tolist()
        return super().default(o)


@ray.remote(num_cpus=0)
def block_until_http_ready(http_endpoint,
                           backoff_time_s=1,
                           check_ready=None,
                           timeout=HTTP_PROXY_TIMEOUT):
    http_is_ready = False
    start_time = time.time()

    while not http_is_ready:
        try:
            resp = requests.get(http_endpoint)
            assert resp.status_code == 200
            if check_ready is None:
                http_is_ready = True
            else:
                http_is_ready = check_ready(resp)
        except Exception:
            pass

        if 0 < timeout < time.time() - start_time:
            raise TimeoutError(
                "HTTP proxy not ready after {} seconds.".format(timeout))

        time.sleep(backoff_time_s)


def get_random_letters(length=6):
    return "".join(random.choices(string.ascii_letters, k=length))


def format_actor_name(actor_name, controller_name=None, *modifiers):
    if controller_name is None:
        name = actor_name
    else:
        name = "{}:{}".format(controller_name, actor_name)

    for modifier in modifiers:
        name += "-{}".format(modifier)

    return name


def get_conda_env_dir(env_name):
    """Given a environment name like `tf1`, find and validate the
    corresponding conda directory. Untested on Windows.
    """
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix is None:
        # The caller is neither in a conda env or in (base).  This is rare
        # because by default, new terminals start in (base), but we can still
        # support this case.
        conda_exe = os.environ.get("CONDA_EXE")
        if conda_exe is None:
            raise RayServeException(
                "Ray Serve cannot find environment variables set by conda. "
                "Please verify conda is installed.")
        # Example: CONDA_EXE=$HOME/anaconda3/bin/python
        # Strip out the /bin/python by going up two parent directories.
        conda_prefix = str(Path(conda_exe).parent.parent)

    # There are two cases:
    # 1. We are in conda base env: CONDA_DEFAULT_ENV=base and
    #    CONDA_PREFIX=$HOME/anaconda3
    # 2. We are in user created conda env: CONDA_DEFAULT_ENV=$env_name and
    #    CONDA_PREFIX=$HOME/anaconda3/envs/$env_name
    if os.environ.get("CONDA_DEFAULT_ENV") == "base":
        # Caller is running in base conda env.
        # Not recommended by conda, but we can still try to support it.
        env_dir = os.path.join(conda_prefix, "envs", env_name)
    else:
        # Now `conda_prefix` should be something like
        # $HOME/anaconda3/envs/$env_name
        # We want to strip the $env_name component.
        conda_envs_dir = os.path.split(conda_prefix)[0]
        env_dir = os.path.join(conda_envs_dir, env_name)
    if not os.path.isdir(env_dir):
        raise ValueError(
            "conda env " + env_name +
            " not found in conda envs directory. Run `conda env list` to " +
            "verify the name is correct.")
    return env_dir


@singledispatch
def chain_future(src, dst):
    """Base method for chaining futures together.

    Chaining futures means the output from source future(s) are written as the
    results of the destination future(s). This method can work with the
    following inputs:
        - src: Future, dst: Future
        - src: List[Future], dst: List[Future]
    """
    raise NotImplementedError()


@chain_future.register(asyncio.Future)
def _chain_future_single(src: asyncio.Future, dst: asyncio.Future):
    asyncio.futures._chain_future(src, dst)


@chain_future.register(list)
def _chain_future_list(src: List[asyncio.Future], dst: List[asyncio.Future]):
    if len(src) != len(dst):
        raise ValueError(
            "Source and destination list doesn't have the same length. "
            "Source: {}. Destination: {}.".foramt(len(src), len(dst)))

    for s, d in zip(src, dst):
        chain_future(s, d)


def unpack_future(src: asyncio.Future, num_items: int) -> List[asyncio.Future]:
    """Unpack the result of source future to num_items futures.

    This function takes in a Future and splits its result into many futures. If
    the result of the source future is an exception, then all destination
    futures will have the same exception.
    """
    dest_futures = [
        asyncio.get_event_loop().create_future() for _ in range(num_items)
    ]

    def unwrap_callback(fut: asyncio.Future):
        exception = fut.exception()
        if exception is not None:
            [f.set_exception(exception) for f in dest_futures]
            return

        result = fut.result()
        assert len(result) == num_items
        for item, future in zip(result, dest_futures):
            future.set_result(item)

    src.add_done_callback(unwrap_callback)

    return dest_futures


def try_schedule_resources_on_nodes(
        requirements: List[dict],
        ray_resource: Dict[str, Dict] = None,
) -> List[bool]:
    """Test given resource requirements can be scheduled on ray nodes.

    Args:
        requirements(List[dict]): The list of resource requirements.
        ray_nodes(Optional[Dict[str, Dict]]): The resource dictionary keyed by
            node id. By default it reads from
            ``ray.state.state._available_resources_per_node()``.
    Returns:
        successfully_scheduled(List[bool]): A list with the same length as
            requirements. Each element indicates whether or not the requirement
            can be satisied.
    """

    if ray_resource is None:
        ray_resource = ray.state.state._available_resources_per_node()

    successfully_scheduled = []

    for resource_dict in requirements:
        # Filter out zero value
        resource_dict = {k: v for k, v in resource_dict.items() if v > 0}

        for node_id, node_resource in ray_resource.items():
            # Check if we can schedule on this node
            feasible = True

            for key, count in resource_dict.items():
                # Fix legacy behaviour in all memory objects
                if "memory" in key:
                    memory_resource = node_resource.get(key, 0)
                    if memory_resource > 0:
                        # Convert from chunks to bytes
                        memory_resource *= MEMORY_RESOURCE_UNIT_BYTES
                    if memory_resource - count < 0:
                        feasible = False

                elif node_resource.get(key, 0) - count < 0:
                    feasible = False

            # If we can, schedule it on this node
            if feasible:
                for key, count in resource_dict.items():
                    node_resource[key] -= count

                successfully_scheduled.append(True)
                break
        else:
            successfully_scheduled.append(False)

    return successfully_scheduled


def get_all_node_ids():
    """Get IDs for all nodes in the cluster.

    Handles multiple nodes on the same IP by appending an index to the
    node_id, e.g., 'node_id-index'.

    Returns a list of ('node_id-index', 'node_id') tuples (the latter can be
    used as a resource requirement for actor placements).
    """
    node_ids = []
    # We need to use the node_id and index here because we could
    # have multiple virtual nodes on the same host. In that case
    # they will have the same IP and therefore node_id.
    for _, node_id_group in groupby(sorted(ray.state.node_ids())):
        for index, node_id in enumerate(node_id_group):
            node_ids.append(("{}-{}".format(node_id, index), node_id))

    return node_ids


def get_node_id_for_actor(actor_handle):
    """Given an actor handle, return the node id it's placed on."""

    return ray.actors()[actor_handle._actor_id.hex()]["Address"]["NodeID"]


def import_class(full_path: str):
    """Given a full import path to a class name, return the imported class.

    For example, the following are equivalent:
        MyClass = import_class("module.submodule.MyClass")
        from module.submodule import MyClass

    Returns:
        Imported class
    """

    last_period_idx = full_path.rfind(".")
    class_name = full_path[last_period_idx + 1:]
    module_name = full_path[:last_period_idx]
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


class MockImportedBackend:
    """Used for testing backends.ImportedBackend.

    This is necessary because we need the class to be installed in the worker
    processes. We could instead mock out importlib but doing so is messier and
    reduces confidence in the test (it isn't truly end-to-end).
    """

    def __init__(self, arg):
        self.arg = arg
        self.config = None

    def reconfigure(self, config):
        self.config = config

    def __call__(self, *args):
        return {"arg": self.arg, "config": self.config}

    async def other_method(self, request):
        return await request.body()


def compute_iterable_delta(old: Iterable,
                           new: Iterable) -> Tuple[set, set, set]:
    """Given two iterables, return the entries that's (added, removed, updated).

    Usage:
        >>> old = {"a", "b"}
        >>> new = {"a", "d"}
        >>> compute_dict_delta(old, new)
        ({"d"}, {"b"}, {"a"})
    """
    old_keys, new_keys = set(old), set(new)
    added_keys = new_keys - old_keys
    removed_keys = old_keys - new_keys
    updated_keys = old_keys.intersection(new_keys)
    return added_keys, removed_keys, updated_keys


def compute_dict_delta(old_dict, new_dict) -> Tuple[dict, dict, dict]:
    """Given two dicts, return the entries that's (added, removed, updated).

    Usage:
        >>> old = {"a": 1, "b": 2}
        >>> new = {"a": 3, "d": 4}
        >>> compute_dict_delta(old, new)
        ({"d": 4}, {"b": 2}, {"a": 3})
    """
    added_keys, removed_keys, updated_keys = compute_iterable_delta(
        old_dict.keys(), new_dict.keys())
    return (
        {k: new_dict[k]
         for k in added_keys},
        {k: old_dict[k]
         for k in removed_keys},
        {k: new_dict[k]
         for k in updated_keys},
    )


def get_current_node_resource_key() -> str:
    """Get the Ray resource key for current node.

    It can be used for actor placement.
    """
    current_node_id = ray.get_runtime_context().node_id.hex()
    for node in ray.nodes():
        if node["NodeID"] == current_node_id:
            # Found the node.
            for key in node["Resources"].keys():
                if key.startswith("node:"):
                    return key
    else:
        raise ValueError("Cannot found the node dictionary for current node.")
