import json
import os
from typing import Dict, Union

from ray._private.runtime_env.constants import (
    RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR,
)
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
def get_archive_paths() -> Union[str, Dict[str, str]]:
    """Return the local paths of archives in the current runtime environment.

    The return value has the same shape as the ``archives`` runtime environment
    field: a string input produces a string path, while a dictionary input
    produces a dictionary containing the same keys and local paths as values.

    The returned paths are local to the current node and should be treated as
    read-only shared cache directories.

    Returns:
        The local archive path, or a dictionary mapping configured names to
        local archive paths.

    Raises:
        RuntimeError: If the current process has no ``archives`` runtime
            environment or its internal path metadata is invalid.
    """
    serialized_paths = os.environ.get(RAY_RUNTIME_ENV_ARCHIVES_PATHS_ENV_VAR)
    if serialized_paths is None:
        raise RuntimeError(
            "No archives are available in this process. Configure the 'archives' "
            "field in the runtime environment before calling get_archive_paths()."
        )

    try:
        paths = json.loads(serialized_paths)
    except json.JSONDecodeError as exc:
        raise RuntimeError("The archives path metadata is not valid JSON.") from exc

    if isinstance(paths, str):
        if paths:
            return paths
    elif (
        isinstance(paths, dict)
        and paths
        and all(
            isinstance(name, str) and name and isinstance(path, str) and path
            for name, path in paths.items()
        )
    ):
        return paths

    raise RuntimeError("The archives path metadata has an invalid value.")
