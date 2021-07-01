from itertools import groupby
import json
import logging
import pickle
import random
import string
import time
from typing import Iterable, Tuple
import os
from collections import UserDict

import starlette.requests
import starlette.responses
import requests
import numpy as np
import pydantic

import ray
import ray.serialization_addons
from ray.util.serialization import StandaloneSerializationContext
from ray.serve.constants import HTTP_PROXY_TIMEOUT
from ray.serve.exceptions import RayServeException
from ray.serve.http_util import build_starlette_request, HTTPRequestWrapper

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
    if len(request_item.args) <= 1:
        arg = request_item.args[0] if len(request_item.args) == 1 else None

        # If the input data from handle is web request, we don't need to wrap
        # it in ServeRequest.
        if isinstance(arg, starlette.requests.Request):
            return (arg, ), {}
        elif request_item.metadata.http_arg_is_pickled:
            assert isinstance(arg, bytes)
            arg: HTTPRequestWrapper = pickle.loads(arg)
            return (build_starlette_request(arg.scope, arg.body), ), {}
        elif request_item.metadata.use_serve_request:
            return (ServeRequest(
                arg,
                request_item.kwargs,
                headers=request_item.metadata.http_headers,
                method=request_item.metadata.http_method,
            ), ), {}

    return request_item.args, request_item.kwargs


class LoggingContext:
    """
    Context manager to manage logging behaviors within a particular block, such as:
    1) Overriding logging level

    Source (python3 official documentation)
    https://docs.python.org/3/howto/logging-cookbook.html#using-a-context-manager-for-selective-logging # noqa: E501
    """

    def __init__(self, logger, level=None):
        self.logger = logger
        self.level = level

    def __enter__(self):
        if self.level is not None:
            self.old_level = self.logger.level
            self.logger.setLevel(self.level)

    def __exit__(self, et, ev, tb):
        if self.level is not None:
            self.logger.setLevel(self.old_level)


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

    return ray.state.actors()[actor_handle._actor_id.hex()]["Address"][
        "NodeID"]


async def mock_imported_function(request):
    return await request.body()


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

    def __call__(self, request):
        return {"arg": self.arg, "config": self.config}

    async def other_method(self, request):
        return await request.body()


def compute_iterable_delta(old: Iterable,
                           new: Iterable) -> Tuple[set, set, set]:
    """Given two iterables, return the entries that's (added, removed, updated).

    Usage:
        >>> old = {"a", "b"}
        >>> new = {"a", "d"}
        >>> compute_iterable_delta(old, new)
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


def ensure_serialization_context():
    """Ensure the serialization addons on registered, even when Ray has not
    been started."""
    ctx = StandaloneSerializationContext()
    ray.serialization_addons.apply(ctx)
