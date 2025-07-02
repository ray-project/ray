import base64
import logging
import os
import traceback
from typing import Any, Callable, Dict, Optional, Union

import ray
import ray._private.ray_constants as ray_constants
import ray.cloudpickle as pickle
from ray._common.utils import load_class
from ray.runtime_env import RuntimeEnv

logger = logging.getLogger(__name__)

RUNTIME_ENV_FUNC_IDENTIFIER = "ray_runtime_env_func::"


def get_import_export_timeout():
    return int(
        os.environ.get(
            ray_constants.RAY_WORKER_PROCESS_SETUP_HOOK_LOAD_TIMEOUT_ENV_VAR, "60"
        )
    )


def _decode_function_key(key: bytes) -> str:
    # b64encode only includes A-Z, a-z, 0-9, + and / characters
    return RUNTIME_ENV_FUNC_IDENTIFIER + base64.b64encode(key).decode()


def _encode_function_key(key: str) -> bytes:
    assert key.startswith(RUNTIME_ENV_FUNC_IDENTIFIER)
    return base64.b64decode(key[len(RUNTIME_ENV_FUNC_IDENTIFIER) :])


def export_setup_func_callable(
    runtime_env: Union[Dict[str, Any], RuntimeEnv],
    setup_func: Callable,
    worker: "ray.Worker",
) -> Union[Dict[str, Any], RuntimeEnv]:
    assert isinstance(setup_func, Callable)
    try:
        key = worker.function_actor_manager.export_setup_func(
            setup_func, timeout=get_import_export_timeout()
        )
    except Exception as e:
        raise ray.exceptions.RuntimeEnvSetupError(
            "Failed to export the setup function."
        ) from e
    env_vars = runtime_env.get("env_vars", {})
    assert ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR not in env_vars, (
        f"The env var, {ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR}, "
        "is not permitted because it is reserved for the internal use."
    )
    env_vars[ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR] = _decode_function_key(
        key
    )
    runtime_env["env_vars"] = env_vars
    # Note: This field is no-op. We don't have a plugin for the setup hook
    # because we can implement it simply using an env var.
    # This field is just for the observability purpose, so we store
    # the name of the method.
    runtime_env["worker_process_setup_hook"] = setup_func.__name__
    return runtime_env


def export_setup_func_module(
    runtime_env: Union[Dict[str, Any], RuntimeEnv],
    setup_func_module: str,
) -> Union[Dict[str, Any], RuntimeEnv]:
    assert isinstance(setup_func_module, str)
    env_vars = runtime_env.get("env_vars", {})
    assert ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR not in env_vars, (
        f"The env var, {ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR}, "
        "is not permitted because it is reserved for the internal use."
    )
    env_vars[ray_constants.WORKER_PROCESS_SETUP_HOOK_ENV_VAR] = setup_func_module
    runtime_env["env_vars"] = env_vars
    return runtime_env


def upload_worker_process_setup_hook_if_needed(
    runtime_env: Union[Dict[str, Any], RuntimeEnv],
    worker: "ray.Worker",
) -> Union[Dict[str, Any], RuntimeEnv]:
    """Uploads the worker_process_setup_hook to GCS with a key.

    runtime_env["worker_process_setup_hook"] is converted to a decoded key
    that can load the worker setup hook function from GCS.
    i.e., you can use internalKV.Get(runtime_env["worker_process_setup_hook])
    to access the worker setup hook from GCS.

    Args:
        runtime_env: The runtime_env. The value will be modified
            when returned.
        worker: ray.worker instance.
        decoder: GCS requires the function key to be bytes. However,
            we cannot json serialize (which is required to serialize
            runtime env) the bytes. So the key should be decoded to
            a string. The given decoder is used to decode the function
            key.
    """
    setup_func = runtime_env.get("worker_process_setup_hook")

    if setup_func is None:
        return runtime_env

    if isinstance(setup_func, Callable):
        return export_setup_func_callable(runtime_env, setup_func, worker)
    elif isinstance(setup_func, str):
        return export_setup_func_module(runtime_env, setup_func)
    else:
        raise TypeError(
            "worker_process_setup_hook must be a function, " f"got {type(setup_func)}."
        )


def load_and_execute_setup_hook(
    worker_process_setup_hook_key: str,
) -> Optional[str]:
    """Load the setup hook from a given key and execute.

    Args:
        worker_process_setup_hook_key: The key to import the setup hook
            from GCS.
    Returns:
        An error message if it fails. None if it succeeds.
    """
    assert worker_process_setup_hook_key is not None
    if not worker_process_setup_hook_key.startswith(RUNTIME_ENV_FUNC_IDENTIFIER):
        return load_and_execute_setup_hook_module(worker_process_setup_hook_key)
    else:
        return load_and_execute_setup_hook_func(worker_process_setup_hook_key)


def load_and_execute_setup_hook_module(
    worker_process_setup_hook_key: str,
) -> Optional[str]:
    try:
        setup_func = load_class(worker_process_setup_hook_key)
        setup_func()
        return None
    except Exception:
        error_message = (
            "Failed to execute the setup hook method, "
            f"{worker_process_setup_hook_key} "
            "from ``ray.init(runtime_env="
            f"{{'worker_process_setup_hook': {worker_process_setup_hook_key}}})``. "
            "Please make sure the given module exists and is available "
            "from ray workers. For more details, see the error trace below.\n"
            f"{traceback.format_exc()}"
        )
        return error_message


def load_and_execute_setup_hook_func(
    worker_process_setup_hook_key: str,
) -> Optional[str]:
    worker = ray._private.worker.global_worker
    assert worker.connected
    func_manager = worker.function_actor_manager
    try:
        worker_setup_func_info = func_manager.fetch_registered_method(
            _encode_function_key(worker_process_setup_hook_key),
            timeout=get_import_export_timeout(),
        )
    except Exception:
        error_message = (
            "Failed to import setup hook within "
            f"{get_import_export_timeout()} seconds.\n"
            f"{traceback.format_exc()}"
        )
        return error_message

    try:
        setup_func = pickle.loads(worker_setup_func_info.function)
    except Exception:
        error_message = (
            "Failed to deserialize the setup hook method.\n" f"{traceback.format_exc()}"
        )
        return error_message

    try:
        setup_func()
    except Exception:
        error_message = (
            f"Failed to execute the setup hook method. Function name:"
            f"{worker_setup_func_info.function_name}\n"
            f"{traceback.format_exc()}"
        )
        return error_message

    return None
