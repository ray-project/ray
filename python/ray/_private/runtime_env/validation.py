import copy
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Union
import yaml

import ray
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import import_attr

# We need to setup this variable before
# using this module
PKG_DIR = None

logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# NOTE(edoakes): we should be able to support up to 512 MiB based on the GCS'
# limit, but for some reason that causes failures when downloading.
GCS_STORAGE_MAX_SIZE = 100 * 1024 * 1024  # 100MiB


def parse_and_validate_working_dir(working_dir: str,
                                   is_task_or_actor: bool = False) -> str:
    """Parses and validates a user-provided 'working_dir' option.

    The working_dir may not be specified per-task or per-actor.

    Otherwise, it should be a valid path to a local directory.
    """
    assert working_dir is not None

    if is_task_or_actor:
        raise NotImplementedError(
            "Overriding working_dir for tasks and actors isn't supported. "
            "Please use ray.init(runtime_env={'working_dir': ...}) "
            "to configure the environment per-job instead.")
    elif not isinstance(working_dir, str):
        raise TypeError("`working_dir` must be a string, got "
                        f"{type(working_dir)}.")
    elif not Path(working_dir).is_dir():
        raise ValueError(
            f"working_dir {working_dir} is not a valid directory.")

    return working_dir


def parse_and_validate_conda(conda: Union[str, dict],
                             is_task_or_actor: bool = False
                             ) -> Union[str, dict]:
    """Parses and validates a user-provided 'conda' option.

    Conda can be one of three cases:
        1) A dictionary describing the env. This is passed through directly.
        2) A string referring to a preinstalled conda env.
        3) A string pointing to a local conda YAML file. This is detected
           by looking for a '.yaml' or '.yml' suffix. In this case, the file
           will be read as YAML and passed through as a dictionary.
    """
    assert conda is not None

    result = None
    if sys.platform == "win32":
        raise NotImplementedError("The 'conda' field in runtime_env "
                                  "is not currently supported on "
                                  "Windows.")
    elif isinstance(conda, str):
        yaml_file = Path(conda)
        if yaml_file.suffix in (".yaml", ".yml"):
            if not yaml_file.is_file():
                raise ValueError(f"Can't find conda YAML file {yaml_file}.")
            try:
                result = yaml.safe_load(yaml_file.read_text())
            except Exception as e:
                raise ValueError(
                    f"Failed to read conda file {yaml_file}: {e}.")
        else:
            # Assume it's a pre-existing conda environment name.
            result = conda
    elif isinstance(conda, dict):
        result = conda
    else:
        raise TypeError("runtime_env['conda'] must be of type str or "
                        f"dict, got {type(conda)}.")

    return result


def parse_and_validate_pip(pip: Union[str, List[str]],
                           is_task_or_actor: bool = False
                           ) -> Optional[List[str]]:
    """Parses and validates a user-provided 'pip' option.

    Conda can be one of two cases:
        1) A List[str] describing the requirements. This is passed through.
        2) A string pointing to a local requirements file. In this case, the
           file contents will be read split into a list.
    """
    assert pip is not None

    result = None
    if sys.platform == "win32":
        raise NotImplementedError("The 'pip' field in runtime_env "
                                  "is not currently supported on "
                                  "Windows.")
    elif isinstance(pip, str):
        # We have been given a path to a requirements.txt file.
        pip_file = Path(pip)
        if not pip_file.is_file():
            raise ValueError(f"{pip_file} is not a valid file")
        result = pip_file.read_text().strip().split("\n")
    elif isinstance(pip, list) and all(isinstance(dep, str) for dep in pip):
        if len(pip) == 0:
            result = None
        else:
            result = pip
    else:
        raise TypeError("runtime_env['pip'] must be of type str or "
                        f"List[str], got {type(pip)}")

    return result


def parse_and_validate_uris(uris: List[str],
                            is_task_or_actor: bool = False) -> List[str]:
    """Parses and validates a user-provided 'uris' option.

    These are passed through without validation (for now).
    """
    assert uris is not None
    return uris


def parse_and_validate_container(container: List[str],
                                 is_task_or_actor: bool = False) -> List[str]:
    """Parses and validates a user-provided 'container' option.

    This is passed through without validation (for now).
    """
    assert container is not None
    return container


def parse_and_validate_env_vars(env_vars: Dict[str, str],
                                is_task_or_actor: bool = False
                                ) -> Optional[Dict[str, str]]:
    """Parses and validates a user-provided 'env_vars' option.

    This is validated to verify that all keys and vals are strings.

    If an empty dictionary is passed, we return `None` for consistency.
    """
    assert env_vars is not None
    if len(env_vars) == 0:
        return None

    if not (isinstance(env_vars, dict) and all(
            isinstance(k, str) and isinstance(v, str)
            for (k, v) in env_vars.items())):
        raise TypeError("runtime_env['env_vars'] must be of type "
                        "Dict[str, str]")

    return env_vars


# Dictionary mapping runtime_env options with the function to parse and
# validate them.
OPTION_TO_VALIDATION_FN = {
    "working_dir": parse_and_validate_working_dir,
    "conda": parse_and_validate_conda,
    "pip": parse_and_validate_pip,
    "uris": parse_and_validate_uris,
    "env_vars": parse_and_validate_env_vars,
    "container": parse_and_validate_container,
}


class ParsedRuntimeEnv(dict):
    """An internal wrapper for runtime_env that is parsed and validated.

    This should be constructed from user-provided input (the API runtime_env)
    and used everywhere that the runtime_env is passed around internally.

    All options in the resulting dictionary will have non-None values.

    Currently supported options:
        working_dir (Path): Specifies the working directory of the worker.
            This can either be a local directory or zip file.
            Examples:
                "."  # cwd
                "local_project.zip"  # archive is unpacked into directory
        uris (List[str]): A list of URIs that define the working_dir.
        pip (List[str] | str): Either a list of pip packages, or a string
            containing the path to a pip requirements.txt file.
        conda (dict | str): Either the conda YAML config, the name of a
            local conda env (e.g., "pytorch_p36"), or the path to a conda
            environment.yaml file.
            The Ray dependency will be automatically injected into the conda
            env to ensure compatibility with the cluster Ray. The conda name
            may be mangled automatically to avoid conflicts between runtime
            envs.
            This field cannot be specified at the same time as the 'pip' field.
            To use pip with conda, please specify your pip dependencies within
            the conda YAML config:
            https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-e
            nvironments.html#create-env-file-manually
            Examples:
                {"channels": ["defaults"], "dependencies": ["codecov"]}
                "pytorch_p36"   # Found on DLAMIs
        container (dict): Require a given (Docker) container image,
            The Ray worker process will run in a container with this image.
            The `worker_path` is the default_worker.py path.
            The `run_options` list spec is here:
            https://docs.docker.com/engine/reference/run/
            Examples:
                {"image": "anyscale/ray-ml:nightly-py38-cpu",
                 "worker_path": "/root/python/ray/workers/default_worker.py",
                 "run_options": ["--cap-drop SYS_ADMIN","--log-level=debug"]}
        env_vars (dict): Environment variables to set.
            Examples:
                {"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}
    """

    known_fields: Set[str] = {
        "working_dir", "conda", "pip", "uris", "containers", "excludes",
        "env_vars", "_ray_release", "_ray_commit", "_inject_current_ray",
        "plugins"
    }

    def __init__(self,
                 runtime_env: Dict[str, Any],
                 is_task_or_actor: bool = False,
                 _validate: bool = True):
        super().__init__()

        # Blindly trust that the runtime_env has already been validated.
        # This is dangerous and should only be used internally (e.g., on the
        # deserialization codepath.
        if not _validate:
            self.update(runtime_env)
            return

        if runtime_env.get("conda") and runtime_env.get("pip"):
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

        for option, validate_fn in OPTION_TO_VALIDATION_FN.items():
            option_val = runtime_env.get(option)
            if option_val is not None:
                validated_option_val = validate_fn(
                    option_val, is_task_or_actor=is_task_or_actor)
                if validated_option_val is not None:
                    self[option] = validated_option_val

        if "_ray_release" in runtime_env:
            self["_ray_release"] = runtime_env["_ray_release"]

        if "_ray_commit" in runtime_env:
            self["_ray_commit"] = runtime_env["_ray_commit"]
        else:
            if self.get("pip") or self.get("conda"):
                self["_ray_commit"] = ray.__commit__

        # Used for testing wheels that have not yet been merged into master.
        # If this is set to True, then we do not inject Ray into the conda
        # or pip dependencies.
        if "_inject_current_ray" in runtime_env:
            self["_inject_current_ray"] = runtime_env["_inject_current_ray"]
        elif "RAY_RUNTIME_ENV_LOCAL_DEV_MODE" in os.environ:
            self["_inject_current_ray"] = True

        if "plugins" in runtime_env:
            self["plugins"] = dict()
            for class_path, plugin_field in runtime_env["plugins"].items():
                plugin_class: RuntimeEnvPlugin = import_attr(class_path)
                if not issubclass(plugin_class, RuntimeEnvPlugin):
                    # TODO(simon): move the inferface to public once ready.
                    raise TypeError(
                        f"{class_path} must be inherit from "
                        "ray._private.runtime_env.plugin.RuntimeEnvPlugin.")
                # TODO(simon): implement uri support.
                _ = plugin_class.validate(runtime_env)
                # Validation passed, add the entry to parsed runtime env.
                self["plugins"][class_path] = plugin_field

        unknown_fields = (
            set(runtime_env.keys()) - ParsedRuntimeEnv.known_fields)
        if len(unknown_fields):
            logger.warning(
                "The following unknown entries in the runtime_env dictionary "
                f"will be ignored: {unknown_fields}. If you intended to use "
                "them as plugins, they must be nested in the `plugins` field.")

        # TODO(architkulkarni) This is to make it easy for the worker caching
        # code in C++ to check if the env is empty without deserializing and
        # parsing it.  We should use a less confusing approach here.
        if all(val is None for val in self.values()):
            self._dict = {}

    @classmethod
    def deserialize(cls, serialized: str) -> "ParsedRuntimeEnv":
        return cls(json.loads(serialized), _validate=False)

    def serialize(self) -> str:
        # Sort the keys we can compare the serialized string for equality.
        return json.dumps(self, sort_keys=True)


def override_task_or_actor_runtime_env(
        child_runtime_env: ParsedRuntimeEnv,
        parent_runtime_env: ParsedRuntimeEnv) -> ParsedRuntimeEnv:
    """Merge the given child runtime env with the parent runtime env.

    If running in a driver, the current runtime env comes from the
    JobConfig.  Otherwise, we are running in a worker for an actor or
    task, and the current runtime env comes from the current TaskSpec.

    By default, the child runtime env inherits non-specified options from the
    parent. There are two exceptions to this:
        - working_dir is not inherited (only URIs).
        - The env_vars dictionaries are merged, so environment variables
          not specified by the child are still inherited from the parent.

    Returns:
        The resulting merged ParsedRuntimeEnv.
    """
    assert child_runtime_env is not None
    assert parent_runtime_env is not None

    # Override environment variables.
    result_env_vars = copy.deepcopy(parent_runtime_env.get("env_vars") or {})
    child_env_vars = child_runtime_env.get("env_vars") or {}
    result_env_vars.update(child_env_vars)

    # Inherit all other non-specified options from the parent.
    result = copy.deepcopy(parent_runtime_env)
    result.update(child_runtime_env)
    if len(result_env_vars) > 0:
        result["env_vars"] = result_env_vars
    if "working_dir" in result:
        del result["working_dir"]  # working_dir should not be in child env.

    # NOTE(architkulkarni): This allows worker caching code in C++ to
    # check if a runtime env is empty without deserializing it.
    assert all(val is not None for val in result.values())

    return result
