import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Union
from pkg_resources import Requirement
from collections import OrderedDict
import yaml

import ray
from ray._private.runtime_env.plugin import RuntimeEnvPlugin, encode_plugin_uri
from ray._private.runtime_env.utils import RuntimeEnv
from ray._private.utils import import_attr
from ray._private.runtime_env.conda import (
    _resolve_install_from_source_ray_extras,
    get_uri as get_conda_uri,
)
from ray._private.runtime_env.pip import RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP

from ray._private.runtime_env.pip import get_uri as get_pip_uri

logger = logging.getLogger(__name__)


def validate_uri(uri: str):
    if not isinstance(uri, str):
        raise TypeError(
            "URIs for working_dir and py_modules must be " f"strings, got {type(uri)}."
        )

    try:
        from ray._private.runtime_env.packaging import parse_uri, Protocol

        protocol, path = parse_uri(uri)
    except ValueError:
        raise ValueError(
            f"{uri} is not a valid URI. Passing directories or modules to "
            "be dynamically uploaded is only supported at the job level "
            "(i.e., passed to `ray.init`)."
        )

    if protocol in Protocol.remote_protocols() and not path.endswith(".zip"):
        raise ValueError("Only .zip files supported for remote URIs.")


def parse_and_validate_py_modules(py_modules: List[str]) -> List[str]:
    """Parses and validates a 'py_modules' option.

    This should be a list of URIs.
    """
    if not isinstance(py_modules, list):
        raise TypeError(
            "`py_modules` must be a list of strings, got " f"{type(py_modules)}."
        )

    for uri in py_modules:
        validate_uri(uri)

    return py_modules


def parse_and_validate_working_dir(working_dir: str) -> str:
    """Parses and validates a 'working_dir' option.

    This should be a URI.
    """
    assert working_dir is not None

    if not isinstance(working_dir, str):
        raise TypeError("`working_dir` must be a string, got " f"{type(working_dir)}.")

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
        raise NotImplementedError(
            "The 'conda' field in runtime_env "
            "is not currently supported on "
            "Windows."
        )
    elif isinstance(conda, str):
        yaml_file = Path(conda)
        if yaml_file.suffix in (".yaml", ".yml"):
            if not yaml_file.is_file():
                raise ValueError(f"Can't find conda YAML file {yaml_file}.")
            try:
                result = yaml.safe_load(yaml_file.read_text())
            except Exception as e:
                raise ValueError(f"Failed to read conda file {yaml_file}: {e}.")
        else:
            # Assume it's a pre-existing conda environment name.
            result = conda
    elif isinstance(conda, dict):
        result = conda
    else:
        raise TypeError(
            "runtime_env['conda'] must be of type str or " f"dict, got {type(conda)}."
        )

    return result


def _rewrite_pip_list_ray_libraries(pip_list: List[str]) -> List[str]:
    result = []
    for specifier in pip_list:
        requirement = Requirement.parse(specifier)
        package_name = requirement.name
        if package_name == "ray":
            libraries = requirement.extras  # e.g. ("serve", "tune")
            if libraries == ():
                # Ray alone was specified (e.g. "ray" or "ray>1.4"). Remove it.
                if os.environ.get(RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP) != "1":
                    logger.warning(
                        "Ray was specified in the `pip` field of the "
                        f"`runtime_env`: '{specifier}'. This is not needed; "
                        "Ray is already installed on the cluster, so that Ray"
                        "installation will be used. To prevent Ray version "
                        f"incompatibility issues, '{specifier}' has been "
                        "deleted from the `pip` field. To disable this "
                        "deletion, set the environment variable "
                        f"{RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP} to 1."
                    )
                else:
                    result.append(specifier)
            else:
                # Replace the library with its dependencies.
                extras = _resolve_install_from_source_ray_extras()
                for library in libraries:
                    result += extras[library]
        else:
            # Pass through all non-Ray packages unmodified.
            result.append(specifier)
    return result


def parse_and_validate_pip(pip: Union[str, List[str]]) -> Optional[List[str]]:
    """Parses and validates a user-provided 'pip' option.

    The value of the input 'pip' field can be one of two cases:
        1) A List[str] describing the requirements. This is passed through.
        2) A string pointing to a local requirements file. In this case, the
           file contents will be read split into a list.

    The returned parsed value will be a list of pip packages. If a Ray library
    (e.g. "ray[serve]") is specified, it will be deleted and replaced by its
    dependencies (e.g. "uvicorn", "requests").

    If the base Ray package (e.g. "ray>1.4" or "ray") is specified in the
    input, it will be removed, unless the environment variable
    RAY_RUNTIME_ENV_ALLOW_RAY_IN_PIP is set to 1.
    """
    assert pip is not None

    pip_list = None
    if sys.platform == "win32":
        raise NotImplementedError(
            "The 'pip' field in runtime_env "
            "is not currently supported on "
            "Windows."
        )
    elif isinstance(pip, str):
        # We have been given a path to a requirements.txt file.
        pip_file = Path(pip)
        if not pip_file.is_file():
            raise ValueError(f"{pip_file} is not a valid file")
        pip_list = pip_file.read_text().strip().split("\n")
    elif isinstance(pip, list) and all(isinstance(dep, str) for dep in pip):
        pip_list = pip
    else:
        raise TypeError(
            "runtime_env['pip'] must be of type str or " f"List[str], got {type(pip)}"
        )

    result = _rewrite_pip_list_ray_libraries(pip_list)

    # Eliminate duplicates to prevent `pip install` from erroring. Use
    # OrderedDict to preserve the order of the list.  This makes the output
    # deterministic and easier to debug, because pip install can have
    # different behavior depending on the order of the input.
    result = list(OrderedDict.fromkeys(result))

    if len(result) == 0:
        result = None

    logger.debug(f"Rewrote runtime_env `pip` field from {pip} to {result}.")

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

    if isinstance(excludes, list) and all(isinstance(path, str) for path in excludes):
        return excludes
    else:
        raise TypeError(
            "runtime_env['excludes'] must be of type "
            f"List[str], got {type(excludes)}"
        )


def parse_and_validate_env_vars(env_vars: Dict[str, str]) -> Optional[Dict[str, str]]:
    """Parses and validates a user-provided 'env_vars' option.

    This is validated to verify that all keys and vals are strings.

    If an empty dictionary is passed, we return `None` for consistency.
    """
    assert env_vars is not None
    if len(env_vars) == 0:
        return None

    if not (
        isinstance(env_vars, dict)
        and all(
            isinstance(k, str) and isinstance(v, str) for (k, v) in env_vars.items()
        )
    ):
        raise TypeError("runtime_env['env_vars'] must be of type " "Dict[str, str]")

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
        self._cached_pb = None

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
                "#create-env-file-manually"
            )

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
                        "ray._private.runtime_env.plugin.RuntimeEnvPlugin."
                    )
                # TODO(simon): implement uri support.
                _ = plugin_class.validate(runtime_env)
                # Validation passed, add the entry to parsed runtime env.
                self["plugins"][class_path] = plugin_field

        unknown_fields = set(runtime_env.keys()) - ParsedRuntimeEnv.known_fields
        if len(unknown_fields):
            logger.warning(
                "The following unknown entries in the runtime_env dictionary "
                f"will be ignored: {unknown_fields}. If you intended to use "
                "them as plugins, they must be nested in the `plugins` field."
            )

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
            plugin_uris.append(encode_plugin_uri("working_dir", self["working_dir"]))
        if "py_modules" in self:
            for uri in self["py_modules"]:
                plugin_uris.append(encode_plugin_uri("py_modules", uri))
        if "conda" in self:
            uri = get_conda_uri(self)
            if uri is not None:
                plugin_uris.append(encode_plugin_uri("conda", uri))
        if "pip" in self:
            uri = get_pip_uri(self)
            if uri is not None:
                plugin_uris.append(encode_plugin_uri("pip", uri))

        return plugin_uris

    def get_proto_runtime_env(self):
        """Return the protobuf structure of runtime env."""
        if self._cached_pb is None:
            self._cached_pb = RuntimeEnv.from_dict(self, get_conda_uri, get_pip_uri)

        return self._cached_pb

    @classmethod
    def deserialize(cls, serialized: str) -> "ParsedRuntimeEnv":
        runtime_env = RuntimeEnv(serialized_runtime_env=serialized)
        return cls(runtime_env.to_dict(), _validate=False)

    def serialize(self) -> str:
        return self.get_proto_runtime_env().serialize()
