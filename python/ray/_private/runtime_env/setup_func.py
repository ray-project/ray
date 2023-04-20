import logging
import hashlib
import base64

from typing import Dict, List, Optional, Any, Callable

import ray
from ray._private.runtime_env.context import RuntimeEnvContext
import ray.cloudpickle as pickle
import ray._private.ray_constants as ray_constants
from ray._private.function_manager import make_function_table_key
from ray._private.utils import check_oversized_function
from ray._raylet import GcsClient
from ray._raylet import JobID
from ray.runtime_env.runtime_env import RuntimeEnv
from ray._private.runtime_env.plugin import RuntimeEnvPlugin

default_logger = logging.getLogger(__name__)


def decode_function_key(key: bytes) -> str:
    return base64.b64encode(key).decode()


def encode_function_key(decoded_key: str) -> bytes:
    return base64.b64decode(decoded_key)


def upload_setup_function_if_needed(
    runtime_env: Dict[str, Any],
    job_id: JobID,
    worker: "ray.Worker",
    gcs_client: GcsClient,
) -> Dict[str, Any]:
    """Uploads the setup_func.
    
    When worker_setup_func is passed to the runtime env,
    instead of implementing the full Plugin, it simply
    updates the env_vars field of the runtime environment
    to pass the __RAY_WORKER_SETUP_FUNC_KEY=<key>,
    where the <key> is the key to obtain serialized
    setup function from GCS.
    """
    setup_func = runtime_env.get("worker_setup_func")
    if setup_func is None:
        return runtime_env

    if not isinstance(setup_func, str) and not isinstance(setup_func, Callable):
        raise TypeError(
            "worker_setup_func must be a string or callable, "
            f"got {type(setup_func)}."
        )

    if isinstance(setup_func, Callable):
        # Upload the function
        # Attempt to pickle the function before we need it. This could
        # fail, and it is more convenient if the failure happens before we
        # actually run the function locally.
        pickled_function = pickle.dumps(setup_func)

        function_to_run_id = hashlib.shake_128(pickled_function).digest(
            ray_constants.ID_SIZE
        )
        key = make_function_table_key(
            b"FunctionsToRun", job_id, function_to_run_id
        )

        check_oversized_function(
            pickled_function, setup_func.__name__, "function", worker
        )

        # Run the function on all workers.
        # SANG-TODO Handle failure.
        gcs_client.internal_kv_put(
            key,
            pickle.dumps(
                {
                    "job_id": job_id.binary(),
                    "function_id": function_to_run_id,
                    "function": pickled_function,
                }
            ),
            True,
            ray_constants.KV_NAMESPACE_FUNCTION_TABLE,
        )
    del runtime_env["worker_setup_func"]
    env_vars = runtime_env.get("env_vars", {})
    assert ray_constants.WORKER_SETUP_FUNC_KEY not in env_vars, (
        f"The env var, {ray_constants.WORKER_SETUP_FUNC_KEY}, "
        "is not permitted because it is reserved for the internal use."
    )
    env_vars[ray_constants.WORKER_SETUP_FUNC_KEY] = decode_function_key(key)
    runtime_env["env_vars"] = env_vars

    return runtime_env
