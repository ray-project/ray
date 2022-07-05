import copy
import importlib
import inspect
import os
import pickle
import random
import string
import time
import traceback
from enum import Enum
from functools import wraps
from itertools import groupby
from typing import Dict, Iterable, List, Tuple

import fastapi.encoders
import numpy as np
import pydantic
import pydantic.json
import requests

import ray
import ray.util.serialization_addons
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray.serve.constants import HTTP_PROXY_TIMEOUT
from ray.serve.http_util import HTTPRequestWrapper, build_starlette_request
from ray.util.serialization import StandaloneSerializationContext

import __main__

try:
    import pandas as pd
except ImportError:
    pd = None

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


class _ServeCustomEncoders:
    """Group of custom encoders for common types that's not handled by FastAPI."""

    @staticmethod
    def encode_np_array(obj):
        assert isinstance(obj, np.ndarray)
        if obj.dtype.kind == "f":  # floats
            obj = obj.astype(float)
        if obj.dtype.kind in {"i", "u"}:  # signed and unsigned integers.
            obj = obj.astype(int)
        return obj.tolist()

    @staticmethod
    def encode_np_scaler(obj):
        assert isinstance(obj, np.generic)
        return obj.item()

    @staticmethod
    def encode_exception(obj):
        assert isinstance(obj, Exception)
        return str(obj)

    @staticmethod
    def encode_pandas_dataframe(obj):
        assert isinstance(obj, pd.DataFrame)
        return obj.to_dict(orient="records")


serve_encoders = {
    np.ndarray: _ServeCustomEncoders.encode_np_array,
    np.generic: _ServeCustomEncoders.encode_np_scaler,
    Exception: _ServeCustomEncoders.encode_exception,
}

if pd is not None:
    serve_encoders[pd.DataFrame] = _ServeCustomEncoders.encode_pandas_dataframe


def install_serve_encoders_to_fastapi():
    """Inject Serve's encoders so FastAPI's jsonable_encoder can pick it up."""
    # https://stackoverflow.com/questions/62311401/override-default-encoders-for-jsonable-encoder-in-fastapi # noqa
    pydantic.json.ENCODERS_BY_TYPE.update(serve_encoders)
    # FastAPI cache these encoders at import time, so we also needs to refresh it.
    fastapi.encoders.encoders_by_class_tuples = (
        fastapi.encoders.generate_encoders_by_class_tuples(
            pydantic.json.ENCODERS_BY_TYPE
        )
    )


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
    for _, node_id_group in groupby(sorted(ray._private.state.node_ids())):
        for index, node_id in enumerate(node_id_group):
            node_ids.append(("{}-{}".format(node_id, index), node_id))

    return node_ids


def node_id_to_ip_addr(node_id: str):
    """Recovers the IP address for an entry from get_all_node_ids."""
    if ":" in node_id:
        node_id = node_id.split(":")[1]

    if "-" in node_id:
        node_id = node_id.split("-")[0]

    return node_id


def get_node_id_for_actor(actor_handle):
    """Given an actor handle, return the node id it's placed on."""

    return ray._private.state.actors()[actor_handle._actor_id.hex()]["Address"][
        "NodeID"
    ]


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
    ray.util.serialization_addons.apply(ctx)


def wrap_to_ray_error(function_name: str, exception: Exception) -> RayTaskError:
    """Utility method to wrap exceptions in user code."""

    try:
        # Raise and catch so we can access traceback.format_exc()
        raise exception
    except Exception as e:
        traceback_str = ray._private.utils.format_error_message(traceback.format_exc())
        return ray.exceptions.RayTaskError(function_name, traceback_str, e)


def msgpack_serialize(obj):
    ctx = ray._private.worker.global_worker.get_serialization_context()
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


def override_runtime_envs_except_env_vars(parent_env: Dict, child_env: Dict) -> Dict:
    """Creates a runtime_env dict by merging a parent and child environment.

    This method is not destructive. It leaves the parent and child envs
    the same.

    The merge is a shallow update where the child environment inherits the
    parent environment's settings. If the child environment specifies any
    env settings, those settings take precdence over the parent.
        - Note: env_vars are a special case. The child's env_vars are combined
            with the parent.

    Args:
        parent_env: The environment to inherit settings from.
        child_env: The environment with override settings.

    Returns: A new dictionary containing the merged runtime_env settings.

    Raises:
        TypeError: If a dictionary is not passed in for parent_env or child_env.
    """

    if not isinstance(parent_env, Dict):
        raise TypeError(
            f'Got unexpected type "{type(parent_env)}" for parent_env. '
            "parent_env must be a dictionary."
        )
    if not isinstance(child_env, Dict):
        raise TypeError(
            f'Got unexpected type "{type(child_env)}" for child_env. '
            "child_env must be a dictionary."
        )

    defaults = copy.deepcopy(parent_env)
    overrides = copy.deepcopy(child_env)

    default_env_vars = defaults.get("env_vars", {})
    override_env_vars = overrides.get("env_vars", {})

    defaults.update(overrides)
    default_env_vars.update(override_env_vars)

    defaults["env_vars"] = default_env_vars

    return defaults


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
        def check_import_once():
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

        if inspect.iscoroutinefunction(func):

            @wraps(func)
            async def wrapped(*args, **kwargs):
                check_import_once()
                return await func(*args, **kwargs)

        elif inspect.isroutine(func):

            @wraps(func)
            def wrapped(*args, **kwargs):
                check_import_once()
                return func(*args, **kwargs)

        else:
            raise ValueError("Decorator expect callable functions.")

        return wrapped

    return decorator


def in_interactive_shell():
    # Taken from:
    # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
    import __main__ as main

    return not hasattr(main, "__file__")
