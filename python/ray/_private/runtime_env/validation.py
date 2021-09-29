import copy
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
import yaml

import ray

# We need to setup this variable before
# using this module
PKG_DIR = None

logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# NOTE(edoakes): we should be able to support up to 512 MiB based on the GCS'
# limit, but for some reason that causes failures when downloading.
GCS_STORAGE_MAX_SIZE = 100 * 1024 * 1024  # 100MiB


def validate_runtime_env(runtime_env: Dict[str, Any],
                         is_task_or_actor: bool = False):
    result = dict()

    working_dir = runtime_env.get("working_dir")
    if working_dir is not None:
        if is_task_or_actor:
            raise NotImplementedError(
                "Overriding working_dir for tasks and actors isn't supported. "
                "Please use ray.init(runtime_env={'working_dir': ...}) "
                "to configure the environment per-job instead.")
        if not isinstance(working_dir, str):
            raise TypeError("`working_dir` must be a string, got "
                            f"{type(working_dir)}.")
        working_dir_path = Path(working_dir)
        if not working_dir_path.is_dir():
            raise ValueError(
                f"working_dir {working_dir} is not a valid directory.")
        result["working_dir"] = working_dir

    conda = runtime_env.get("conda")
    if conda is not None:
        if sys.platform == "win32":
            raise NotImplementedError("The 'conda' field in runtime_env "
                                      "is not currently supported on "
                                      "Windows.")

        if isinstance(conda, str):
            yaml_file = Path(conda)
            if yaml_file.suffix in (".yaml", ".yml"):
                if not yaml_file.is_file():
                    raise ValueError(
                        f"Can't find conda YAML file {yaml_file}.")
                try:
                    result["conda"] = yaml.safe_load(yaml_file.read_text())
                except Exception as e:
                    raise ValueError(
                        f"Failed to read conda file {yaml_file}: {e}.")
            else:
                # Assume it's a pre-existing conda environment name.
                result["conda"] = conda
        elif isinstance(conda, dict):
            result["conda"] = conda
        elif conda is not None:
            raise TypeError("runtime_env['conda'] must be of type str or "
                            f"dict, got {type(conda)}.")

    pip = runtime_env.get("pip")
    if pip is not None:
        if sys.platform == "win32":
            raise NotImplementedError("The 'pip' field in runtime_env "
                                      "is not currently supported on "
                                      "Windows.")
        if runtime_env.get("conda") is not None:
            raise ValueError(
                "The 'pip' field and 'conda' field of "
                "runtime_env cannot both be specified.\n"
                f"specified pip field: {runtime_env['pip']}\n"
                f"specified conda field: {runtime_env['conda']}\n"
                "To use pip with conda, please only set the 'conda' "
                "field, and specify your pip dependencies "
                "within the conda YAML config dict: see "
                "https://conda.io/projects/conda/en/latest/"
                "user-guide/tasks/manage-environments.html"
                "#create-env-file-manually")

        if isinstance(pip, str):
            # We have been given a path to a requirements.txt file.
            pip_file = Path(pip)
            if not pip_file.is_file():
                raise ValueError(f"{pip_file} is not a valid file")
            result["pip"] = pip_file.read_text()
        elif isinstance(pip, list) and all(
                isinstance(dep, str) for dep in pip):
            # Construct valid pip requirements.txt from list of packages.
            result["pip"] = "\n".join(pip) + "\n"
        else:
            raise TypeError("runtime_env['pip'] must be of type str or "
                            f"List[str], got {type(pip)}")

    if "uris" in runtime_env:
        result["uris"] = runtime_env["uris"]

    if "container" in runtime_env:
        result["container"] = runtime_env["container"]

    env_vars = runtime_env.get("env_vars")
    if env_vars is not None:
        if not (isinstance(env_vars, dict) and all(
                isinstance(k, str) and isinstance(v, str)
                for (k, v) in env_vars.items())):
            raise TypeError("runtime_env['env_vars'] must be of type "
                            "Dict[str, str]")

        if len(env_vars) > 0:
            result["env_vars"] = env_vars

    if "_ray_release" in runtime_env:
        result["_ray_release"] = runtime_env["_ray_release"]

    if "_ray_commit" in runtime_env:
        result["_ray_commit"] = runtime_env["_ray_commit"]
    else:
        if result.get("pip") or result.get("conda"):
            result["_ray_commit"] = ray.__commit__

    # Used for testing wheels that have not yet been merged into master.
    # If this is set to True, then we do not inject Ray into the conda
    # or pip dependencies.
    if "_inject_current_ray" in runtime_env:
        result["_inject_current_ray"] = runtime_env["_inject_current_ray"]
    elif "RAY_RUNTIME_ENV_LOCAL_DEV_MODE" in os.environ:
        result["_inject_current_ray"] = True

    # NOTE(architkulkarni): This allows worker caching code in C++ to
    # check if a runtime env is empty without deserializing it.
    assert all(val is not None for val in result.values())

    return result


def override_task_or_actor_runtime_env(
        child_runtime_env: Optional[Dict[str, Any]],
        parent_runtime_env: Dict[str, Any]) -> Dict[str, Any]:
    """Merge the given new runtime env with the current runtime env.

    If running in a driver, the current runtime env comes from the
    JobConfig.  Otherwise, we are running in a worker for an actor or
    task, and the current runtime env comes from the current TaskSpec.

    The child's runtime env dict is merged with the parents via a simple
    dict update, except for runtime_env["env_vars"], which is merged
    with runtime_env["env_vars"] of the parent rather than overwriting it.
    This is so that env vars set in the parent propagate to child actors
    and tasks even if a new env var is set in the child.

    Args:
        runtime_env_dict (dict): A runtime env for a child actor or task.

    Returns:
        The resulting merged JSON-serialized runtime env.
    """

    result = copy.deepcopy(child_runtime_env)

    # If per-actor URIs aren't specified, override them with those in the
    # job config.
    if "uris" not in result and "uris" in parent_runtime_env:
        result["uris"] = parent_runtime_env.get("uris")

    # Override environment variables.
    result_env_vars = copy.deepcopy(parent_runtime_env.get("env_vars") or {})
    child_env_vars = child_runtime_env.get("env_vars") or {}
    result_env_vars.update(child_env_vars)
    if len(result_env_vars) > 0:
        result["env_vars"] = result_env_vars

    # NOTE(architkulkarni): This allows worker caching code in C++ to
    # check if a runtime env is empty without deserializing it.
    assert all(val is not None for val in result.values())

    return result
