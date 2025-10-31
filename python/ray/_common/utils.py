import asyncio
import binascii
import errno
import importlib
import inspect
import os
import random
import string
import sys
import tempfile
import time
from abc import ABC, abstractmethod
from inspect import signature
from types import ModuleType
from typing import Any, Coroutine, Dict, Optional, Tuple

import psutil


def import_module_and_attr(
    full_path: str, *, reload_module: bool = False
) -> Tuple[ModuleType, Any]:
    """Given a full import path to a module attr, return the imported module and attr.

    If `reload_module` is set, the module will be reloaded using `importlib.reload`.

    Args:
        full_path: The full import path to the module and attr.
        reload_module: Whether to reload the module.

    Returns:
        A tuple of the imported module and attr.
    """
    if ":" in full_path:
        if full_path.count(":") > 1:
            raise ValueError(
                f'Got invalid import path "{full_path}". An '
                "import path may have at most one colon."
            )
        module_name, attr_name = full_path.split(":")
    else:
        last_period_idx = full_path.rfind(".")
        module_name = full_path[:last_period_idx]
        attr_name = full_path[last_period_idx + 1 :]
    module = importlib.import_module(module_name)
    if reload_module:
        importlib.reload(module)
    return module, getattr(module, attr_name)


def import_attr(full_path: str, *, reload_module: bool = False) -> Any:
    """Given a full import path to a module attr, return the imported attr.

    If `reload_module` is set, the module will be reloaded using `importlib.reload`.

    For example, the following are equivalent:
        MyClass = import_attr("module.submodule:MyClass")
        MyClass = import_attr("module.submodule.MyClass")
        from module.submodule import MyClass

    Returns:
        Imported attr
    """
    return import_module_and_attr(full_path, reload_module=reload_module)[1]


def get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """Get a running async event loop if one exists, otherwise create one.

    This function serves as a proxy for the deprecating get_event_loop().
    It tries to get the running loop first, and if no running loop
    could be retrieved:
    - For python version <3.10: it falls back to the get_event_loop
        call.
    - For python version >= 3.10: it uses the same python implementation
        of _get_event_loop() at asyncio/events.py.

    Ideally, one should use high level APIs like asyncio.run() with python
    version >= 3.7, if not possible, one should create and manage the event
    loops explicitly.
    """
    vers_info = sys.version_info
    if vers_info.major >= 3 and vers_info.minor >= 10:
        # This follows the implementation of the deprecating `get_event_loop`
        # in python3.10's asyncio. See python3.10/asyncio/events.py
        # _get_event_loop()
        try:
            loop = asyncio.get_running_loop()
            assert loop is not None
            return loop
        except RuntimeError as e:
            # No running loop, relying on the error message as for now to
            # differentiate runtime errors.
            assert "no running event loop" in str(e)
            return asyncio.get_event_loop_policy().get_event_loop()

    return asyncio.get_event_loop()


_BACKGROUND_TASKS = set()


def run_background_task(coroutine: Coroutine) -> asyncio.Task:
    """Schedule a task reliably to the event loop.

    This API is used when you don't want to cache the reference of `asyncio.Task`.
    For example,

    ```
    get_event_loop().create_task(coroutine(*args))
    ```

    The above code doesn't guarantee to schedule the coroutine to the event loops

    When using create_task in a  "fire and forget" way, we should keep the references
    alive for the reliable execution. This API is used to fire and forget
    asynchronous execution.

    https://docs.python.org/3/library/asyncio-task.html#creating-tasks
    """
    task = get_or_create_event_loop().create_task(coroutine)
    # Add task to the set. This creates a strong reference.
    _BACKGROUND_TASKS.add(task)

    # To prevent keeping references to finished tasks forever,
    # make each task remove its own reference from the set after
    # completion:
    task.add_done_callback(_BACKGROUND_TASKS.discard)
    return task


# Used in gpu detection
RESOURCE_CONSTRAINT_PREFIX = "accelerator_type:"
PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME = "bundle"


def resources_from_ray_options(options_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Determine a task's resource requirements.

    Args:
        options_dict: The dictionary that contains resources requirements.

    Returns:
        A dictionary of the resource requirements for the task.
    """
    resources = (options_dict.get("resources") or {}).copy()

    if "CPU" in resources or "GPU" in resources:
        raise ValueError(
            "The resources dictionary must not contain the key 'CPU' or 'GPU'"
        )
    elif "memory" in resources or "object_store_memory" in resources:
        raise ValueError(
            "The resources dictionary must not "
            "contain the key 'memory' or 'object_store_memory'"
        )
    elif PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME in resources:
        raise ValueError(
            "The resource should not include `bundle` which "
            f"is reserved for Ray. resources: {resources}"
        )

    num_cpus = options_dict.get("num_cpus")
    num_gpus = options_dict.get("num_gpus")
    memory = options_dict.get("memory")
    object_store_memory = options_dict.get("object_store_memory")
    accelerator_type = options_dict.get("accelerator_type")

    if num_cpus is not None:
        resources["CPU"] = num_cpus
    if num_gpus is not None:
        resources["GPU"] = num_gpus
    if memory is not None:
        resources["memory"] = int(memory)
    if object_store_memory is not None:
        resources["object_store_memory"] = object_store_memory
    if accelerator_type is not None:
        resources[f"{RESOURCE_CONSTRAINT_PREFIX}{accelerator_type}"] = 0.001

    return resources


# Match the standard alphabet used for UUIDs.
RANDOM_STRING_ALPHABET = string.ascii_lowercase + string.digits


def get_random_alphanumeric_string(length: int):
    """Generates random string of length consisting exclusively of
    - Lower-case ASCII chars
    - Digits
    """
    return "".join(random.choices(RANDOM_STRING_ALPHABET, k=length))


_PRINTED_WARNING = set()


def get_call_location(back: int = 1):
    """
    Get the location (filename and line number) of a function caller, `back`
    frames up the stack.

    Args:
        back: The number of frames to go up the stack, not including this
            function.

    Returns:
        A string with the filename and line number of the caller.
        For example, "myfile.py:123".
    """
    stack = inspect.stack()
    try:
        frame = stack[back + 1]
        return f"{frame.filename}:{frame.lineno}"
    except IndexError:
        return "UNKNOWN"


def get_user_temp_dir():
    if "RAY_TMPDIR" in os.environ:
        return os.environ["RAY_TMPDIR"]
    elif sys.platform.startswith("linux") and "TMPDIR" in os.environ:
        return os.environ["TMPDIR"]
    elif sys.platform.startswith("darwin") or sys.platform.startswith("linux"):
        # Ideally we wouldn't need this fallback, but keep it for now for
        # for compatibility
        tempdir = os.path.join(os.sep, "tmp")
    else:
        tempdir = tempfile.gettempdir()
    return tempdir


def get_ray_temp_dir():
    return os.path.join(get_user_temp_dir(), "ray")


def get_ray_address_file(temp_dir: Optional[str]):
    if temp_dir is None:
        temp_dir = get_ray_temp_dir()
    return os.path.join(temp_dir, "ray_current_cluster")


def reset_ray_address(temp_dir: Optional[str] = None):
    address_file = get_ray_address_file(temp_dir)
    if os.path.exists(address_file):
        try:
            os.remove(address_file)
        except OSError:
            pass


def load_class(path):
    """Load a class at runtime given a full path.

    Example of the path: mypkg.mysubpkg.myclass
    """
    class_data = path.split(".")
    if len(class_data) < 2:
        raise ValueError("You need to pass a valid path like mymodule.provider_class")
    module_path = ".".join(class_data[:-1])
    class_str = class_data[-1]
    module = importlib.import_module(module_path)
    return getattr(module, class_str)


def get_system_memory(
    # For cgroups v1:
    memory_limit_filename: str = "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    # For cgroups v2:
    memory_limit_filename_v2: str = "/sys/fs/cgroup/memory.max",
):
    """Return the total amount of system memory in bytes.

    Args:
        memory_limit_filename: The path to the file that contains the memory
            limit for the Docker container. Defaults to
            /sys/fs/cgroup/memory/memory.limit_in_bytes.
        memory_limit_filename_v2: The path to the file that contains the memory
            limit for the Docker container in cgroups v2. Defaults to
            /sys/fs/cgroup/memory.max.

    Returns:
        The total amount of system memory in bytes.
    """
    # Try to accurately figure out the memory limit if we are in a docker
    # container. Note that this file is not specific to Docker and its value is
    # often much larger than the actual amount of memory.
    docker_limit = None
    if os.path.exists(memory_limit_filename):
        with open(memory_limit_filename, "r") as f:
            docker_limit = int(f.read().strip())
    elif os.path.exists(memory_limit_filename_v2):
        with open(memory_limit_filename_v2, "r") as f:
            # Don't forget to strip() the newline:
            max_file = f.read().strip()
            if max_file.isnumeric():
                docker_limit = int(max_file)
            else:
                # max_file is "max", i.e. is unset.
                docker_limit = None

    # Use psutil if it is available.
    psutil_memory_in_bytes = psutil.virtual_memory().total

    if docker_limit is not None:
        # We take the min because the cgroup limit is very large if we aren't
        # in Docker.
        return min(docker_limit, psutil_memory_in_bytes)

    return psutil_memory_in_bytes


def binary_to_hex(identifier):
    hex_identifier = binascii.hexlify(identifier)
    hex_identifier = hex_identifier.decode()
    return hex_identifier


def hex_to_binary(hex_identifier):
    return binascii.unhexlify(hex_identifier)


def try_make_directory_shared(directory_path):
    try:
        os.chmod(directory_path, 0o0777)
    except OSError as e:
        # Silently suppress the PermissionError that is thrown by the chmod.
        # This is done because the user attempting to change the permissions
        # on a directory may not own it. The chmod is attempted whether the
        # directory is new or not to avoid race conditions.
        # ray-project/ray/#3591
        if e.errno in [errno.EACCES, errno.EPERM]:
            pass
        else:
            raise


def try_to_create_directory(directory_path):
    # Attempt to create a directory that is globally readable/writable.
    directory_path = os.path.expanduser(directory_path)
    os.makedirs(directory_path, exist_ok=True)
    # Change the log directory permissions so others can use it. This is
    # important when multiple people are using the same machine.
    try_make_directory_shared(directory_path)


def get_function_args(callable):
    all_parameters = frozenset(signature(callable).parameters)
    return list(all_parameters)


def decode(byte_str: str, allow_none: bool = False, encode_type: str = "utf-8"):
    """Make this unicode in Python 3, otherwise leave it as bytes.

    Args:
        byte_str: The byte string to decode.
        allow_none: If true, then we will allow byte_str to be None in which
            case we will return an empty string. TODO(rkn): Remove this flag.
            This is only here to simplify upgrading to flatbuffers 1.10.0.
        encode_type: The encoding type to use for decoding. Defaults to "utf-8".

    Returns:
        A byte string in Python 2 and a unicode string in Python 3.
    """
    if byte_str is None and allow_none:
        return ""

    if not isinstance(byte_str, bytes):
        raise ValueError(f"The argument {byte_str} must be a bytes object.")
    return byte_str.decode(encode_type)


class TimerBase(ABC):
    @abstractmethod
    def time(self) -> float:
        """Return the current time."""
        raise NotImplementedError


class Timer(TimerBase):
    def time(self) -> float:
        return time.time()
