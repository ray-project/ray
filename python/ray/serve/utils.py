from functools import wraps
import importlib
from itertools import groupby
import json
import logging
import pickle
import random
import string
import time
from typing import Iterable, List, Tuple
import os
import traceback
from enum import Enum
import __main__
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
)

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
        >>> from ray.serve.utils import compute_iterable_delta
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
        >>> from ray.serve.utils import compute_dict_delta
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


def get_deployment_import_path(
    deployment, replace_main=False, enforce_importable=False
):
    """
    Gets the import path for deployment's func_or_class.

    deployment: A deployment object whose import path should be returned
    replace_main: If this is True, the function will try to replace __main__
        with __main__'s file name if the deployment's module is __main__
    """

    body = deployment._func_or_class

    if isinstance(body, str):
        # deployment's func_or_class is already an import path
        return body
    elif hasattr(body, "__ray_actor_class__"):
        # If ActorClass, get the class or function inside
        body = body.__ray_actor_class__

    import_path = f"{body.__module__}.{body.__qualname__}"

    if enforce_importable and "<locals>" in body.__qualname__:
        raise RuntimeError(
            "Deployment definitions must be importable to build the Serve app, "
            f"but deployment '{deployment.name}' is inline defined or returned "
            "from another function. Please restructure your code so that "
            f"'{import_path}' can be imported (i.e., put it in a module)."
        )

    if replace_main:
        # Replaces __main__ with its file name. E.g. suppose the import path
        # is __main__.classname and classname is defined in filename.py.
        # Its import path becomes filename.classname.

        if import_path.split(".")[0] == "__main__" and hasattr(__main__, "__file__"):
            file_name = os.path.basename(__main__.__file__)
            extensionless_file_name = file_name.split(".")[0]
            attribute_name = import_path.split(".")[-1]
            import_path = f"{extensionless_file_name}.{attribute_name}"

    return import_path


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
        >>> from ray.serve.utils import require_packages
        >>> @require_packages(["numpy", "package_a"]) # doctest: +SKIP
        ... def func(): # doctest: +SKIP
        ...     import numpy as np # doctest: +SKIP
        ...     ... # doctest: +SKIP
        >>> func() # doctest: +SKIP
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


def in_interactive_shell():
    # Taken from:
    # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
    import __main__ as main

    return not hasattr(main, "__file__")
