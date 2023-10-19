import asyncio
import copy
import importlib
import inspect
import logging
import math
import os
import random
import string
import threading
import time
import traceback
from abc import ABC, abstractmethod
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import numpy as np
import requests

import ray
import ray.util.serialization_addons
from ray._private.resource_spec import HEAD_NODE_RESOURCE_NAME
from ray._private.utils import import_attr
from ray._private.worker import LOCAL_MODE, SCRIPT_MODE
from ray._raylet import MessagePackSerializer
from ray.actor import ActorHandle
from ray.exceptions import RayTaskError
from ray.serve._private.constants import HTTP_PROXY_TIMEOUT, SERVE_LOGGER_NAME
from ray.types import ObjectRef
from ray.util.serialization import StandaloneSerializationContext

try:
    import pandas as pd
except ImportError:
    pd = None

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
        >>> from ray.serve._private.utils import require_packages
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


def check_obj_ref_ready_nowait(obj_ref: ObjectRef) -> bool:
    """Check if ray object reference is ready without waiting for it."""
    finished, _ = ray.wait([obj_ref], timeout=0)
    return len(finished) == 1


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


class _MetricTask:
    def __init__(self, task_func, interval_s, callback_func):
        """
        Args:
            task_func: a callable that MetricsPusher will try to call in each loop.
            interval_s: the interval of each task_func is supposed to be called.
            callback_func: callback function is called when task_func is done, and
                the result of task_func is passed to callback_func as the first
                argument, and the timestamp of the call is passed as the second
                argument.
        """
        self.task_func: Callable = task_func
        self.interval_s: float = interval_s
        self.callback_func: Callable[[Any, float]] = callback_func
        self.last_ref: Optional[ray.ObjectRef] = None
        self.last_call_succeeded_time: Optional[float] = time.time()


class MetricsPusher:
    """
    Metrics pusher is a background thread that run the registered tasks in a loop.
    """

    def __init__(
        self,
    ):
        self.tasks: List[_MetricTask] = []
        self.pusher_thread: Union[threading.Thread, None] = None
        self.stop_event = threading.Event()

    def register_task(self, task_func, interval_s, process_func=None):
        self.tasks.append(_MetricTask(task_func, interval_s, process_func))

    def start(self):
        """Start a background thread to run the registered tasks in a loop.

        We use this background so it will be not blocked by user's code and ensure
        consistently metrics delivery. Python GIL will ensure that this thread gets
        fair timeshare to execute and run.
        """

        def send_forever():
            while True:
                if self.stop_event.is_set():
                    return

                start = time.time()
                for task in self.tasks:
                    try:
                        if start - task.last_call_succeeded_time >= task.interval_s:
                            if task.last_ref:
                                ready_refs, _ = ray.wait([task.last_ref], timeout=0)
                                if len(ready_refs) == 0:
                                    continue
                            data = task.task_func()
                            task.last_call_succeeded_time = time.time()
                            if task.callback_func and ray.is_initialized():
                                task.last_ref = task.callback_func(
                                    data, send_timestamp=time.time()
                                )
                    except Exception as e:
                        logger.warning(
                            f"MetricsPusher thread failed to run metric task: {e}"
                        )

                # For all tasks, check when the task should be executed
                # next. Sleep until the next closest time.
                least_interval_s = math.inf
                for task in self.tasks:
                    time_until_next_push = task.interval_s - (
                        time.time() - task.last_call_succeeded_time
                    )
                    least_interval_s = min(least_interval_s, time_until_next_push)

                time.sleep(max(least_interval_s, 0))

        if len(self.tasks) == 0:
            raise ValueError("MetricsPusher has zero tasks registered.")

        self.pusher_thread = threading.Thread(target=send_forever)
        # Making this a daemon thread so it doesn't leak upon shutdown, and it
        # doesn't need to block the replica's shutdown.
        self.pusher_thread.setDaemon(True)
        self.pusher_thread.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        """Shutdown metrics pusher gracefully.

        This method will ensure idempotency of shutdown call.
        """
        if not self.stop_event.is_set():
            self.stop_event.set()

        if self.pusher_thread:
            self.pusher_thread.join()


def call_function_from_import_path(import_path: str) -> Any:
    """Call the function given import path.

    Args:
        import_path: The import path of the function to call.
    Raises:
        ValueError: If the import path is invalid.
        TypeError: If the import path is not callable.
        RuntimeError: if the function raise exeception during execution.
    Returns:
        The result of the function call.
    """
    try:
        callback_func = import_attr(import_path)
    except Exception as e:
        raise ValueError(f"The import path {import_path} cannot be imported: {e}")

    if not callable(callback_func):
        raise TypeError(f"The import path {import_path} is not callable.")

    try:
        return callback_func()
    except Exception as e:
        raise RuntimeError(f"The function {import_path} raised an exception: {e}")


def get_head_node_id() -> str:
    """Get the head node id.

    Iterate through all nodes in the ray cluster and return the node id of the first
    alive node with head node resource.
    """
    head_node_id = None
    for node in ray.nodes():
        if HEAD_NODE_RESOURCE_NAME in node["Resources"] and node["Alive"]:
            head_node_id = node["NodeID"]
            break
    assert head_node_id is not None, "Cannot find alive head node."

    return head_node_id


def calculate_remaining_timeout(
    *,
    timeout_s: Optional[float],
    start_time_s: float,
    curr_time_s: float,
) -> Optional[float]:
    """Get the timeout remaining given an overall timeout, start time, and curr time.

    If the timeout passed in was `None` or negative, will always return that timeout
    directly.

    If the timeout is >= 0, the returned remaining timeout always be >= 0.
    """
    if timeout_s is None or timeout_s < 0:
        return timeout_s

    time_since_start_s = curr_time_s - start_time_s
    return max(0, timeout_s - time_since_start_s)


def get_all_live_placement_group_names() -> List[str]:
    """Fetch and parse the Ray placement group table for live placement group names.

    Placement groups are filtered based on their `scheduling_state`; any placement
    group not in the "REMOVED" state is considered live.
    """
    placement_group_table = ray.util.placement_group_table()

    live_pg_names = []
    for entry in placement_group_table.values():
        pg_name = entry.get("name", "")
        if (
            pg_name
            and entry.get("stats", {}).get("scheduling_state", "UNKNOWN") != "REMOVED"
        ):
            live_pg_names.append(pg_name)

    return live_pg_names


def get_current_actor_id() -> str:
    """Gets the ID of the calling actor.

    If this is called in a driver, returns "DRIVER."

    If otherwise called outside of an actor, returns an empty string.

    This function hangs when GCS is down due to the `ray.get_runtime_context()`
    call.
    """

    worker_mode = ray.get_runtime_context().worker.mode
    if worker_mode in {SCRIPT_MODE, LOCAL_MODE}:
        return "DRIVER"
    else:
        try:
            actor_id = ray.get_runtime_context().get_actor_id()
            if actor_id is None:
                return ""
            else:
                return actor_id
        except Exception:
            return ""


def is_running_in_asyncio_loop() -> bool:
    try:
        asyncio.get_running_loop()
        return True
    except RuntimeError:
        return False


class TimerBase(ABC):
    @abstractmethod
    def time(self) -> float:
        """Return the current time."""
        raise NotImplementedError


class Timer(TimerBase):
    def time(self) -> float:
        return time.time()
