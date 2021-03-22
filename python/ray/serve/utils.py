import asyncio
from functools import singledispatch
import importlib
from itertools import groupby
import json
import logging
import random
import string
import time
from typing import Iterable, List, Tuple, Dict, Optional
import os
from ray.serve.exceptions import RayServeException
from collections import UserDict

import starlette.requests
import starlette.responses
import requests
import numpy as np
import pydantic

import ray
from ray.serve.constants import HTTP_PROXY_TIMEOUT

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


def import_attr(full_path: str):
    """Given a full import path to a module attr, return the imported attr.

    For example, the following are equivalent:
        MyClass = import_attr("module.submodule.MyClass")
        from module.submodule import MyClass

    Returns:
        Imported attr
    """

    last_period_idx = full_path.rfind(".")
    attr_name = full_path[last_period_idx + 1:]
    module_name = full_path[:last_period_idx]
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


async def mock_imported_function(batch):
    result = []
    for request in batch:
        result.append(await request.body())
    return result


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

    def __call__(self, batch):
        return [{
            "arg": self.arg,
            "config": self.config
        } for _ in range(len(batch))]

    async def other_method(self, batch):
        responses = []
        for request in batch:
            responses.append(await request.body())
        return responses


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


def register_custom_serializers():
    """Install custom serializers needed for Ray Serve."""
    import starlette.datastructures
    import pydantic.fields

    assert ray.is_initialized(
    ), "This functional must be ran with Ray initialized."

    # Pydantic's Cython validators are not serializable.
    # https://github.com/cloudpipe/cloudpickle/issues/408
    ray.worker.global_worker.run_function_on_all_workers(
        lambda _: ray.util.register_serializer(
            pydantic.fields.ModelField,
            serializer=lambda o: {
                "name": o.name,
                "type_": o.type_,
                "class_validators": o.class_validators,
                "model_config": o.model_config,
                "default": o.default,
                "default_factory": o.default_factory,
                "required": o.required,
                "alias": o.alias,
                "field_info": o.field_info,
            },
            deserializer=lambda kwargs: pydantic.fields.ModelField(**kwargs),
        )
    )

    # FastAPI's app.state object is not serializable
    # because it overrides __getattr__
    ray.worker.global_worker.run_function_on_all_workers(
        lambda _: ray.util.register_serializer(
            starlette.datastructures.State,
            serializer=lambda s: s._state,
            deserializer=lambda s: starlette.datastructures.State(s),
        )
    )


class ASGIHTTPSender:
    """Implement the interface for ASGI sender, build Starlette Response"""

    def __init__(self) -> None:
        self.status_code: Optional[int] = 200
        self.header: Dict[str, str] = {}
        self.buffer: List[bytes] = []

    async def __call__(self, message):
        if (message["type"] == "http.response.start"):
            self.status_code = message["status"]
            for key, value in message["headers"]:
                self.header[key.decode()] = value.decode()
        elif (message["type"] == "http.response.body"):
            self.buffer.append(message["body"])
        else:
            raise ValueError("ASGI type must be one of "
                             "http.responses.{body,start}.")

    def build_starlette_response(self) -> starlette.responses.Response:
        return starlette.responses.Response(
            b"".join(self.buffer),
            status_code=self.status_code,
            headers=dict(self.header))
