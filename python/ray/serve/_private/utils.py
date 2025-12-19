import asyncio
import collections
import copy
import importlib
import inspect
import logging
import random
import re
import time
import uuid
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

import requests

import ray
import ray.util.serialization_addons
from ray._common.constants import HEAD_NODE_RESOURCE_NAME
from ray._common.utils import get_random_alphanumeric_string, import_attr
from ray._private.worker import LOCAL_MODE, SCRIPT_MODE
from ray._raylet import MessagePackSerializer
from ray.actor import ActorHandle
from ray.serve._private.common import RequestMetadata, ServeComponentType
from ray.serve._private.constants import HTTP_PROXY_TIMEOUT, SERVE_LOGGER_NAME
from ray.types import ObjectRef
from ray.util.serialization import StandaloneSerializationContext

try:
    import pandas as pd
except ImportError:
    pd = None

try:
    import numpy as np
except ImportError:
    np = None

FILE_NAME_REGEX = r"[^\x20-\x7E]|[<>:\"/\\|?*]"

MESSAGE_PACK_OFFSET = 9


def validate_ssl_config(
    ssl_certfile: Optional[str], ssl_keyfile: Optional[str]
) -> None:
    """Validate SSL configuration for HTTPS support.

    Args:
        ssl_certfile: Path to SSL certificate file
        ssl_keyfile: Path to SSL private key file

    Raises:
        ValueError: If only one of ssl_certfile or ssl_keyfile is provided
    """
    if (ssl_certfile and not ssl_keyfile) or (ssl_keyfile and not ssl_certfile):
        raise ValueError(
            "Both ssl_keyfile and ssl_certfile must be provided together "
            "to enable HTTPS."
        )


GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR = RuntimeError(
    "Streaming deployment handle results cannot be passed to "
    "downstream handle calls. If you have a use case requiring "
    "this feature, please file a feature request on GitHub."
)


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

# Format for component files
FILE_FMT = "{component_name}_{component_id}{suffix}"


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


serve_encoders = {Exception: _ServeCustomEncoders.encode_exception}

if np is not None:
    serve_encoders[np.ndarray] = _ServeCustomEncoders.encode_np_array
    serve_encoders[np.generic] = _ServeCustomEncoders.encode_np_scaler

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


def get_random_string(length: int = 8):
    return get_random_alphanumeric_string(length)


def format_actor_name(actor_name, *modifiers):
    name = actor_name
    for modifier in modifiers:
        name += "-{}".format(modifier)

    return name


CLASS_WRAPPER_METADATA_ATTRS = (
    "__name__",
    "__qualname__",
    "__module__",
    "__doc__",
    "__annotations__",
)


def copy_class_metadata(wrapper_cls, target_cls) -> None:
    """Copy common class-level metadata onto a wrapper class."""
    for attr in CLASS_WRAPPER_METADATA_ATTRS:
        if attr == "__annotations__":
            target_annotations = getattr(target_cls, "__annotations__", None)
            if target_annotations:
                merged_annotations = dict(
                    wrapper_cls.__dict__.get("__annotations__", {})
                )
                for key, value in target_annotations.items():
                    merged_annotations.setdefault(key, value)
                wrapper_cls.__annotations__ = merged_annotations
            continue

        if hasattr(target_cls, attr):
            setattr(wrapper_cls, attr, getattr(target_cls, attr))
    wrapper_cls.__wrapped__ = target_cls


def ensure_serialization_context():
    """Ensure the serialization addons on registered, even when Ray has not
    been started."""
    ctx = StandaloneSerializationContext()
    ray.util.serialization_addons.apply(ctx)


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
                func._require_packages_checked = True

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


def snake_to_camel_case(snake_str: str) -> str:
    """Convert a snake case string to camel case."""

    words = snake_str.strip("_").split("_")
    return words[0] + "".join(word[:1].upper() + word[1:] for word in words[1:])


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


def get_capacity_adjusted_num_replicas(
    num_replicas: int, target_capacity: Optional[float]
) -> int:
    """Return the `num_replicas` adjusted by the `target_capacity`.

    The output will only ever be 0 if `target_capacity` is 0 or `num_replicas` is
    0 (to support autoscaling deployments using scale-to-zero).

    Rather than using the default `round` behavior in Python, which rounds half to
    even, uses the `decimal` module to round half up (standard rounding behavior).
    """
    if target_capacity is None or target_capacity == 100:
        return num_replicas

    if target_capacity == 0 or num_replicas == 0:
        return 0

    adjusted_num_replicas = Decimal(num_replicas * target_capacity) / Decimal(100.0)
    rounded_adjusted_num_replicas = adjusted_num_replicas.to_integral_value(
        rounding=ROUND_HALF_UP
    )
    return max(1, int(rounded_adjusted_num_replicas))


def generate_request_id() -> str:
    # NOTE(edoakes): we use random.getrandbits because it reduces CPU overhead
    # significantly. This is less cryptographically secure but should be ok for
    # request ID generation.
    # See https://bugs.python.org/issue45556 for discussion.
    return str(uuid.UUID(int=random.getrandbits(128), version=4))


def inside_ray_client_context() -> bool:
    return ray.util.client.ray.is_connected()


def get_component_file_name(
    component_name: str,
    component_id: str,
    component_type: Optional[ServeComponentType],
    suffix: str = "",
) -> str:
    """Get the component's file name. Replaces special characters with underscores."""
    component_name = re.sub(FILE_NAME_REGEX, "_", component_name)

    # For DEPLOYMENT component type, we want to log the deployment name
    # instead of adding the component type to the component name.
    component_log_file_name = component_name
    if component_type is not None:
        component_log_file_name = f"{component_type.value}_{component_name}"
        if component_type != ServeComponentType.REPLICA:
            component_name = f"{component_type}_{component_name}"
    file_name = FILE_FMT.format(
        component_name=component_log_file_name,
        component_id=component_id,
        suffix=suffix,
    )
    return file_name


def validate_route_prefix(route_prefix: Union[DEFAULT, None, str]):
    if route_prefix is DEFAULT.VALUE or route_prefix is None:
        return

    if not route_prefix.startswith("/"):
        raise ValueError(
            f"Invalid route_prefix '{route_prefix}', "
            "must start with a forward slash ('/')."
        )

    if route_prefix != "/" and route_prefix.endswith("/"):
        raise ValueError(
            f"Invalid route_prefix '{route_prefix}', "
            "may not end with a trailing '/'."
        )

    if "{" in route_prefix or "}" in route_prefix:
        raise ValueError(
            f"Invalid route_prefix '{route_prefix}', may not contain wildcards."
        )


async def resolve_deployment_response(obj: Any, request_metadata: RequestMetadata):
    """Resolve `DeploymentResponse` objects to underlying object references.

    This enables composition without explicitly calling `_to_object_ref`.
    """
    from ray.serve.handle import DeploymentResponse, DeploymentResponseGenerator

    if isinstance(obj, DeploymentResponseGenerator):
        raise GENERATOR_COMPOSITION_NOT_SUPPORTED_ERROR
    elif isinstance(obj, DeploymentResponse):
        # Launch async task to convert DeploymentResponse to an object ref
        return asyncio.create_task(obj._to_object_ref())


def wait_for_interrupt() -> None:
    try:
        while True:
            # Block, letting Ray print logs to the terminal.
            time.sleep(10)
    except KeyboardInterrupt:
        logger.warning("Got KeyboardInterrupt, exiting...")
        # We need to re-raise KeyboardInterrupt, so serve components can be shutdown
        # from the main script.
        raise


def is_grpc_enabled(grpc_config) -> bool:
    return grpc_config.port > 0 and len(grpc_config.grpc_servicer_functions) > 0


class Semaphore:
    """Based on asyncio.Semaphore.

    This is a semaphore that can be used to limit the number of concurrent requests.
    Its maximum value is dynamic and is determined by the `get_value_fn` function.
    """

    def __init__(self, get_value_fn: Callable[[], int]):
        self._waiters = None
        self._value = 0
        self._get_value_fn = get_value_fn

    def __repr__(self):
        res = super().__repr__()
        extra = "locked" if self.locked() else f"unlocked, value:{self._value}"
        if self._waiters:
            extra = f"{extra}, waiters:{len(self._waiters)}"
        return f"<{res[1:-1]} [{extra}]>"

    async def __aenter__(self):
        await self.acquire()
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    async def __aexit__(self, exc_type, exc, tb):
        self.release()

    def get_max_value(self):
        return self._get_value_fn()

    def locked(self):
        """Returns True if semaphore cannot be acquired immediately."""
        return self._value >= self.get_max_value() or (
            any(not w.cancelled() for w in (self._waiters or ()))
        )

    async def acquire(self):
        """Acquire a semaphore.
        If the internal counter is larger than zero on entry,
        decrement it by one and return True immediately.  If it is
        zero on entry, block, waiting until some other coroutine has
        called release() to make it larger than 0, and then return
        True.
        """
        if not self.locked():
            self._value += 1
            return True

        if self._waiters is None:
            self._waiters = collections.deque()
        fut = asyncio.Future()
        self._waiters.append(fut)

        # Finally block should be called before the CancelledError
        # handling as we don't want CancelledError to call
        # _wake_up_first() and attempt to wake up itself.
        try:
            try:
                await fut
            finally:
                self._waiters.remove(fut)
        except asyncio.CancelledError:
            if not fut.cancelled():
                self._value -= 1
                self._wake_up_next()
            raise

        if self._value < self.get_max_value():
            self._wake_up_next()
        return True

    def release(self):
        """Release a semaphore, incrementing the internal counter by one.
        When it was zero on entry and another coroutine is waiting for it to
        become larger than zero again, wake up that coroutine.
        """
        self._value -= 1
        self._wake_up_next()

    def _wake_up_next(self):
        """Wake up the first waiter that isn't done."""
        if not self._waiters:
            return

        for fut in self._waiters:
            if not fut.done():
                self._value += 1
                fut.set_result(True)
                return
