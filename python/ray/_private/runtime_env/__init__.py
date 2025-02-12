import json
import os
from typing import Any, Dict


def runtime_env_override_hook(runtime_env: Dict[str, Any]) -> Dict[str, Any]:
    """Hook to override the runtime environment from RAY_OVERRIDE_RUNTIME_ENV_JSON."""

    if "RAY_OVERRIDE_RUNTIME_ENV_JSON" in os.environ:
        runtime_env = json.loads(os.environ["RAY_OVERRIDE_RUNTIME_ENV_JSON"])

    return runtime_env


# List of files to exclude from the Ray directory when using runtime_env for
# Ray development. These are not necessary in the Ray workers.
RAY_WORKER_DEV_EXCLUDES = ["raylet", "gcs_server", "cpp/", "tests/", "core/src"]
