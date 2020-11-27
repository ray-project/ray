import asyncio
from functools import singledispatch
from itertools import groupby
import json
import logging
import random
import string
import time
from typing import List, Dict
import io
import os
from ray.serve.exceptions import RayServeException
from collections import UserDict

import requests
import numpy as np
import pydantic
import flask

import ray
from ray.serve.constants import HTTP_PROXY_TIMEOUT
from ray.serve.context import TaskContext
from ray.serve.http_util import build_flask_request

ACTOR_FAILURE_RETRY_TIMEOUT_S = 60


class ServeMultiDict(UserDict):
    """Compatible data structure to simulate Flask.Request.args API."""

    def getlist(self, key):
        """Return the list of items for a given key."""
        return self.data.get(key, [])


class ServeRequest:
    """The request object used in Python context.

    ServeRequest is built to have similar API as Flask.Request. You only need
    to write your model serving code once; it can be queried by both HTTP and
    Python.
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
    def args(self):
        """The keyword arguments from ``handle.remote(**kwargs)``."""
        return self._kwargs

    @property
    def json(self):
        """The request dictionary, from ``handle.remote(dict)``."""
        if not isinstance(self._data, dict):
            raise RayServeException("Request data is not a dictionary. "
                                    f"It is {type(self._data)}.")
        return self._data

    @property
    def form(self):
        """The request dictionary, from ``handle.remote(dict)``."""
        if not isinstance(self._data, dict):
            raise RayServeException("Request data is not a dictionary. "
                                    f"It is {type(self._data)}.")
        return self._data

    @property
    def data(self):
        """The request data from ``handle.remote(obj)``."""
        return self._data


def parse_request_item(request_item):
    if request_item.metadata.request_context == TaskContext.Web:
        asgi_scope, body_bytes = request_item.args
        return build_flask_request(asgi_scope, io.BytesIO(body_bytes))
    else:
        arg = request_item.args[0] if len(request_item.args) == 1 else None

        # If the input data from handle is web request, we don't need to wrap
        # it in ServeRequest.
        if isinstance(arg, flask.Request):
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
    if os.environ.get("SERVE_LOG_DEBUG"):
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
                           timeout=HTTP_PROXY_TIMEOUT):
    http_is_ready = False
    start_time = time.time()

    while not http_is_ready:
        try:
            resp = requests.get(http_endpoint)
            assert resp.status_code == 200
            http_is_ready = True
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
    corresponding conda directory.
    """
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix is None:
        raise ValueError(
            "Serve cannot find environment variables installed by conda. " +
            "Are you sure you are in a conda env?")

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
                if node_resource.get(key, 0) - count < 0:
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
