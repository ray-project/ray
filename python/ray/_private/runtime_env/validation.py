import copy
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Union, Tuple
import yaml

import ray
from ray._private.runtime_env.plugin import RuntimeEnvPlugin
from ray._private.utils import import_attr
from ray._private.runtime_env import conda

logger = logging.getLogger(__name__)


def _encode_plugin_uri(plugin: str, uri: str) -> str:
    return plugin + "|" + uri


def _decode_plugin_uri(plugin_uri: str) -> Tuple[str, str]:
    if "|" not in plugin_uri:
        raise ValueError(
            f"Plugin URI must be of the form 'plugin|uri', not {plugin_uri}")
    return tuple(plugin_uri.split("|", 2))


def validate_uri(uri: str):
    if not isinstance(uri, str):
        raise TypeError("URIs for working_dir and py_modules must be "
                        f"strings, got {type(uri)}.")

    try:
        from ray._private.runtime_env.packaging import parse_uri, Protocol
        protocol, path = parse_uri(uri)
    except ValueError:
        raise ValueError(
            f"{uri} is not a valid URI. Passing directories or modules to "
            "be dynamically uploaded is only supported at the job level "
            "(i.e., passed to `ray.init`).")

    if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
        raise ValueError("Only .zip files supported for remote URIs.")


def parse_and_validate_py_modules(py_modules: List[str]) -> List[str]:
    """Parses and validates a 'py_modules' option.

    This should be a list of URIs.
    """
    if not isinstance(py_modules, list):
        raise TypeError("`py_modules` must be a list of strings, got "
                        f"{type(py_modules)}.")

    for uri in py_modules:
        validate_uri(uri)

    return py_modules


def parse_and_validate_working_dir(working_dir: str) -> str:
    """Parses and validates a 'working_dir' option.

    This should be a URI.
    """
    assert working_dir is not None

    if not isinstance(working_dir, str):
        raise TypeError("`working_dir` must be a string, got "
                        f"{type(working_dir)}.")

    validate_uri(working_dir)

    return working_dir


def parse_and_validate_conda(conda: Union[str, dict]) -> Union[str, dict]:
    """Parses and validates a user-provided 'conda' option.

    Conda can be one of three cases:
        1) A dictionary describing the env. This is passed through directly.
        2) A string referring to the name of a preinstalled conda env.
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


def parse_and_validate_pip(pip: Union[str, List[str]]) -> Optional[List[str]]:
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


def parse_and_validate_container(container: List[str]) -> List[str]:
    """Parses and validates a user-provided 'container' option.

    This is passed through without validation (for now).
    """
    assert container is not None
    return container


def parse_and_validate_excludes(excludes: List[str]) -> List[str]:
    """Parses and validates a user-provided 'excludes' option.

    This is validated to verify that it is of type List[str].

    If an empty list is passed, we return `None` for consistency.
    """
    assert excludes is not None

    if isinstance(excludes, list) and len(excludes) == 0:
        return None

    if (isinstance(excludes, list)
            and all(isinstance(path, str) for path in excludes)):
        return excludes
    else:
        raise TypeError("runtime_env['excludes'] must be of type "
                        f"List[str], got {type(excludes)}")


def parse_and_validate_env_vars(
        env_vars: Dict[str, str]) -> Optional[Dict[str, str]]:
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
    "py_modules": parse_and_validate_py_modules,
    "working_dir": parse_and_validate_working_dir,
    "excludes": parse_and_validate_excludes,
    "conda": parse_and_validate_conda,
    "pip": parse_and_validate_pip,
    "env_vars": parse_and_validate_env_vars,
    "container": parse_and_validate_container,
}


class ParsedRuntimeEnv(dict):
    """An internal wrapper for runtime_env that is parsed and validated.

    This should be constructed from user-provided input (the API runtime_env)
    and used everywhere that the runtime_env is passed around internally.

    All options in the resulting dictionary will have non-None values.

    Currently supported options:
        py_modules (List[URI]): List of URIs (either in the GCS or external
            storage), each of which is a zip file that will be unpacked and
            inserted into the PYTHONPATH of the workers.
        working_dir (URI): URI (either in the GCS or external storage) of a zip
            file that will be unpacked in the directory of each task/actor.
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
        "py_modules",
        "working_dir",
        "conda",
        "pip",
        "container",
        "excludes",
        "env_vars",
        "_ray_release",
        "_ray_commit",
        "_inject_current_ray",
        "plugins",
        "eager_install",
    }

    def __init__(self, runtime_env: Dict[str, Any], _validate: bool = True):
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
                validated_option_val = validate_fn(option_val)
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

        # NOTE(architkulkarni): This allows worker caching code in C++ to check
        # if a runtime env is empty without deserializing it.  This is a catch-
        # all; for validated inputs we won't set the key if the value is None.
        if all(val is None for val in self.values()):
            self.clear()

    def get_uris(self) -> List[str]:
        # TODO(architkulkarni): this should programmatically be extended with
        # URIs from all plugins.
        plugin_uris = []
        if "working_dir" in self:
            plugin_uris.append(
                _encode_plugin_uri("working_dir", self["working_dir"]))
        if "py_modules" in self:
            for uri in self["py_modules"]:
                plugin_uris.append(_encode_plugin_uri("py_modules", uri))
        if "conda" or "pip" in self:
            uri = conda.get_uri(self)
            if uri is not None:
                plugin_uris.append(_encode_plugin_uri("conda", uri))

        return plugin_uris

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
    parent. There is one exception to this:
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

    # NOTE(architkulkarni): This allows worker caching code in C++ to
    # check if a runtime env is empty without deserializing it.
    assert all(val is not None for val in result.values())

    return result
