from functools import wraps
import importlib
from itertools import groupby
import json
import logging
import pickle
import random
import string
import time
from typing import Iterable, List, Tuple, Dict, Any
import os
import traceback
from enum import Enum
from ray.actor import ActorHandle

import requests
import numpy as np
import pydantic

import ray
import ray.serialization_addons
from ray.exceptions import RayTaskError
from ray.util.serialization import StandaloneSerializationContext
from ray.serve.http_util import build_starlette_request, HTTPRequestWrapper
from ray.serve.constants import (
    HTTP_PROXY_TIMEOUT,
    SERVE_HANDLE_JSON_KEY,
    ServeHandleType,
)
from ray import serve

ACTOR_FAILURE_RETRY_TIMEOUT_S = 60


# Use a global singleton enum to emulate default options. We cannot use None
# for those option because None is a valid new value.
class DEFAULT(Enum):
    VALUE = 1


def parse_request_item(request_item):
    if len(request_item.args) == 1:
        arg = request_item.args[0]
        if request_item.metadata.http_arg_is_pickled:
            assert isinstance(arg, bytes)
            arg: HTTPRequestWrapper = pickle.loads(arg)
            return (build_starlette_request(arg.scope, arg.body),), {}

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


class ServeHandleEncoder(json.JSONEncoder):
    """JSON encoder for RayServeHandle and RayServeSyncHandle. Use to enforce
    JSON serialization of deployment init args & kwargs to faciliate serve
    pipeline deployment as well as operationaling serve.
    """

    def default(self, obj):
        # Import RayServeHandle in utils file lead to import errors
        if type(obj).__name__ == "RayServeSyncHandle":
            return {
                SERVE_HANDLE_JSON_KEY: ServeHandleType.SYNC,
                "deployment_name": obj.deployment_name,
                "_internal_pickled_http_request": obj._pickled_http_request,
            }
        elif type(obj).__name__ == "RayServeHandle":
            return {
                SERVE_HANDLE_JSON_KEY: ServeHandleType.ASYNC,
                "deployment_name": obj.deployment_name,
                "_internal_pickled_http_request": obj._pickled_http_request,
            }
        else:
            return super().default(obj)


def serve_handle_object_hook(ray_serve_handle_json: Dict[str, Any]):
    """Return RayServeHandle given a JSON serialized dict. Re-constructs the
    object by fullfilling the following fieds that matches our signature of
    `get_handle()`:
        - controller handle
        - deployment name
        - _internal_pickled_http_request
    """

    if SERVE_HANDLE_JSON_KEY in ray_serve_handle_json:
        is_sync = (
            True
            if ray_serve_handle_json[SERVE_HANDLE_JSON_KEY] == ServeHandleType.SYNC
            else False
        )
        return serve.api._get_global_client().get_handle(
            ray_serve_handle_json["deployment_name"],
            sync=is_sync,
            missing_ok=True,
            _internal_pickled_http_request=ray_serve_handle_json[
                "_internal_pickled_http_request"
            ],
        )
    else:
        # Not RayServeHandle type.
        try:
            return json.loads(ray_serve_handle_json)
        except Exception:
            return ray_serve_handle_json


@ray.remote(num_cpus=0)
def block_until_http_ready(
    http_endpoint,
    backoff_time_s=1,
    check_ready=None,
    timeout=HTTP_PROXY_TIMEOUT,
):
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
            raise TimeoutError("HTTP proxy not ready after {} seconds.".format(timeout))

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

    return ray.state.actors()[actor_handle._actor_id.hex()]["Address"]["NodeID"]


def compute_iterable_delta(old: Iterable, new: Iterable) -> Tuple[set, set, set]:
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
        old_dict.keys(), new_dict.keys()
    )
    return (
        {k: new_dict[k] for k in added_keys},
        {k: old_dict[k] for k in removed_keys},
        {k: new_dict[k] for k in updated_keys},
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


def wrap_to_ray_error(function_name: str, exception: Exception) -> RayTaskError:
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray._private.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(function_name, traceback_str, e)


def msgpack_serialize(obj):
    ctx = ray.worker.global_worker.get_serialization_context()
    buffer = ctx.serialize(obj)
    serialized = buffer.to_bytes()
    return serialized


def parse_import_path(import_path: str):
    """
    Takes in an import_path of form:

    [subdirectory 1].[subdir 2]...[subdir n].[file name].[attribute name]

    Parses this path and returns the module name (everything before the last
    dot) and attribute name (everything after the last dot), such that the
    attribute can be imported using "from module_name import attr_name".
    """

    nodes = import_path.split(".")
    if len(nodes) < 2:
        raise ValueError(
            f"Got {import_path} as import path. The import path "
            f"should at least specify the file name and "
            f"attribute name connected by a dot."
        )

    return ".".join(nodes[:-1]), nodes[-1]


class JavaActorHandleProxy:
    """Wraps actor handle and translate snake_case to camelCase."""

    def __init__(self, handle: ActorHandle):
        self.handle = handle
        self._available_attrs = set(dir(self.handle))

    def __getattr__(self, key: str):
        if key in self._available_attrs:
            camel_case_key = key
        else:
            components = key.split("_")
            camel_case_key = components[0] + "".join(x.title() for x in components[1:])
        return getattr(self.handle, camel_case_key)


def require_packages(packages: List[str]):
    """Decorator making sure function run in specified environments

    Examples:
        >>> @require_packages(["numpy", "package_a"])
            def func():
                import numpy as np
        >>> func()
            ImportError: func requires ["numpy", "package_a"] but
            ["package_a"] are not available, please pip install them.
    """

    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            if not hasattr(func, "_require_packages_checked"):
                missing_packages = []
                for package in packages:
                    try:
                        importlib.import_module(package)
                    except ModuleNotFoundError:
                        missing_packages.append(package)
                if len(missing_packages) > 0:
                    raise ImportError(
                        f"{func} requires packages {packages} to run but "
                        f"{missing_packages} are missing. Please "
                        "`pip install` them or add them to "
                        "`runtime_env`."
                    )
                setattr(func, "_require_packages_checked", True)
            return func(*args, **kwargs)

        return wrapped

    return decorator
