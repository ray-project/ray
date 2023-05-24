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
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Tuple,
    TypeVar,
    Union,
    Optional,
)
import threading

import fastapi.encoders
import numpy as np
import pydantic
import pydantic.json
import requests
import logging

import ray
import ray.util.serialization_addons
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray.serve._private.constants import (
    HTTP_PROXY_TIMEOUT,
    RAY_GCS_RPC_TIMEOUT_S,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.http_util import HTTPRequestWrapper, build_starlette_request
from ray.util.serialization import StandaloneSerializationContext
from ray._raylet import MessagePackSerializer
from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

import __main__

try:
    import pandas as pd
except ImportError:
    pd = None

ACTOR_FAILURE_RETRY_TIMEOUT_S = 60
MESSAGE_PACK_OFFSET = 9


# Use a global singleton enum to emulate default options. We cannot use None
# for those option because None is a valid new value.
class DEFAULT(Enum):
    VALUE = 1


class DeploymentOptionUpdateType(str, Enum):
    # Nothing needs to be done other than setting the target state.
    LightWeight = "LightWeight"
    # Each DeploymentReplica instance (tracked in DeploymentState) uses certain options
    # from the deployment config. These values need to be updated in DeploymentReplica.
    NeedsReconfigure = "NeedsReconfigure"
    # Options that are sent to the replica actor. If changed, reconfigure() on the actor
    # needs to be called to update these values.
    NeedsActorReconfigure = "NeedsActorReconfigure"
    # If changed, restart all replicas.
    HeavyWeight = "HeavyWeight"


# Type alias: objects that can be DEFAULT.VALUE have type Default[T]
T = TypeVar("T")
Default = Union[DEFAULT, T]

logger = logging.getLogger(SERVE_LOGGER_NAME)


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


def get_all_node_ids(gcs_client) -> List[Tuple[str, str]]:
    """Get IDs for all live nodes in the cluster.

    Returns a list of (node_id: str, ip_address: str). The node_id can be
    passed into the Ray SchedulingPolicy API.
    """
    nodes = gcs_client.get_all_node_info(timeout=RAY_GCS_RPC_TIMEOUT_S)
    node_ids = [
        (ray.NodeID.from_binary(node_id).hex(), node["node_name"].decode("utf-8"))
        for (node_id, node) in nodes.items()
        if node["state"] == ray.core.generated.gcs_pb2.GcsNodeInfo.ALIVE
    ]

    # Sort on NodeID to ensure the ordering is deterministic across the cluster.
    sorted(node_ids)
    return node_ids


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


def msgpack_deserialize(data):
    # todo: Ray does not provide a msgpack deserialization api.
    try:
        obj = MessagePackSerializer.loads(data[MESSAGE_PACK_OFFSET:], None)
    except Exception:
        raise
    return obj


def merge_dict(dict1, dict2):
    if dict1 is None and dict2 is None:
        return None
    if dict1 is None:
        dict1 = dict()
    if dict2 is None:
        dict2 = dict()
    result = dict()
    for key in dict1.keys() | dict2.keys():
        result[key] = sum([e.get(key, 0) for e in (dict1, dict2)])
    return result


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


def guarded_deprecation_warning(*args, **kwargs):
    """Wrapper for deprecation warnings, guarded by a flag."""
    if os.environ.get("SERVE_WARN_V1_DEPRECATIONS", "0") == "1":
        from ray._private.utils import deprecated

        return deprecated(*args, **kwargs)
    else:

        def noop_decorator(func):
            return func

        return noop_decorator


def snake_to_camel_case(snake_str: str) -> str:
    """Convert a snake case string to camel case."""

    words = snake_str.strip("_").split("_")
    return words[0] + "".join(word[:1].upper() + word[1:] for word in words[1:])


def dict_keys_snake_to_camel_case(snake_dict: dict) -> dict:
    """Converts dictionary's keys from snake case to camel case.

    Does not modify original dictionary.
    """

    camel_dict = dict()

    for key, val in snake_dict.items():
        if isinstance(key, str):
            camel_dict[snake_to_camel_case(key)] = val
        else:
            camel_dict[key] = val

    return camel_dict


serve_telemetry_tag_map = {
    "SERVE_API_VERSION": TagKey.SERVE_API_VERSION,
    "SERVE_NUM_DEPLOYMENTS": TagKey.SERVE_NUM_DEPLOYMENTS,
    "GCS_STORAGE": TagKey.GCS_STORAGE,
    "SERVE_NUM_GPU_DEPLOYMENTS": TagKey.SERVE_NUM_GPU_DEPLOYMENTS,
    "SERVE_FASTAPI_USED": TagKey.SERVE_FASTAPI_USED,
    "SERVE_DAG_DRIVER_USED": TagKey.SERVE_DAG_DRIVER_USED,
    "SERVE_HTTP_ADAPTER_USED": TagKey.SERVE_HTTP_ADAPTER_USED,
    "SERVE_GRPC_INGRESS_USED": TagKey.SERVE_GRPC_INGRESS_USED,
    "SERVE_REST_API_VERSION": TagKey.SERVE_REST_API_VERSION,
    "SERVE_NUM_APPS": TagKey.SERVE_NUM_APPS,
    "SERVE_NUM_REPLICAS_LIGHTWEIGHT_UPDATED": (
        TagKey.SERVE_NUM_REPLICAS_LIGHTWEIGHT_UPDATED
    ),
    "SERVE_USER_CONFIG_LIGHTWEIGHT_UPDATED": (
        TagKey.SERVE_USER_CONFIG_LIGHTWEIGHT_UPDATED
    ),
    "SERVE_AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED": (
        TagKey.SERVE_AUTOSCALING_CONFIG_LIGHTWEIGHT_UPDATED
    ),
}


def record_serve_tag(key: str, value: str):
    """Record telemetry.

    TagKey objects cannot be pickled, so deployments can't directly record
    telemetry using record_extra_usage_tag. They can instead call this function
    which records telemetry for them.
    """

    if key not in serve_telemetry_tag_map:
        raise ValueError(
            f'The TagKey "{key}" does not exist. Expected a key from: '
            f"{list(serve_telemetry_tag_map.keys())}."
        )

    record_extra_usage_tag(serve_telemetry_tag_map[key], value)


def extract_self_if_method_call(args: List[Any], func: Callable) -> Optional[object]:
    """Check if this is a method rather than a function.

    Does this by checking to see if `func` is the attribute of the first
    (`self`) argument under `func.__name__`. Unfortunately, this is the most
    robust solution to this I was able to find. It would also be preferable
    to do this check when the decorator runs, rather than when the method is.

    Returns the `self` object if it's a method call, else None.

    Arguments:
        args: arguments to the function/method call.
        func: the unbound function that was called.
    """
    if len(args) > 0:
        method = getattr(args[0], func.__name__, False)
        if method:
            wrapped = getattr(method, "__wrapped__", False)
            if wrapped and wrapped == func:
                return args[0]

    return None


class MetricsPusher:
    def __init__(
        self,
        metrics_process_func: Callable,
        interval_s: float,
        collection_callback: Callable,
    ):
        """
        Args:
            interval_s: the push interval.
            collection_callback: a callable that returns the metric data points to
            be sent to the the controller. The collection callback should take
            no argument and returns a dictionary of str_key -> float_value.
            metrics_process_func: actor handle function.
        """
        self.collection_callback = collection_callback
        self.metrics_process_func = metrics_process_func
        self.interval_s = interval_s
        self.pusher_thread: Union[threading.Thread, None] = None
        self.stop_event = threading.Event()

    def start(self):
        """Start a background thread to push metrics to controller.

        We use this background so it will be not blocked by user's code and ensure
        consistently metrics delivery. Python GIL will ensure that this thread gets
        fair timeshare to execute and run.
        """

        def send_once():
            data = self.collection_callback()

            # TODO(simon): maybe wait for ack or handle controller failure?
            return self.metrics_process_func(data=data, send_timestamp=time.time())

        def send_forever():
            last_ref: Optional[ray.ObjectRef] = None
            last_send_succeeded: bool = True

            while True:
                start = time.time()
                if self.stop_event.is_set():
                    return

                if ray.is_initialized():
                    try:
                        if last_ref:
                            ready_refs, _ = ray.wait([last_ref], timeout=0)
                            last_send_succeeded = len(ready_refs) == 1
                        if last_send_succeeded:
                            last_ref = send_once()
                    except Exception as e:
                        logger.warning(
                            "Autoscaling metrics pusher thread "
                            "is failing to send metrics to the controller "
                            f": {e}"
                        )

                duration_s = time.time() - start
                remaining_time = self.interval_s - duration_s
                if remaining_time > 0:
                    time.sleep(remaining_time)

        self.pusher_thread = threading.Thread(target=send_forever)
        # Making this a daemon thread so it doesn't leak upon shutdown, and it
        # doesn't need to block the replica's shutdown.
        self.pusher_thread.setDaemon(True)
        self.pusher_thread.start()

    def __del__(self):
        self.stop_event.set()
        self.pusher_thread.join()
