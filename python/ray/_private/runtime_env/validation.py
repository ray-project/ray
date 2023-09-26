import logging
from pathlib import Path
import sys
from typing import Dict, List, Optional, Union

from collections import OrderedDict
import yaml

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

    if (
        protocol in Protocol.remote_protocols()
        and not path.endswith(".zip")
        and not path.endswith(".whl")
    ):
        raise ValueError("Only .zip or .whl files supported for remote URIs.")


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

    if sys.platform == "win32":
        logger.warning(
            "runtime environment support is experimental on Windows. "
            "If you run into issues please file a report at "
            "https://github.com/ray-project/ray/issues."
        )

    result = None
    if isinstance(conda, str):
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


def parse_and_validate_pip(pip: Union[str, List[str], Dict]) -> Optional[Dict]:
    """Parses and validates a user-provided 'pip' option.

    The value of the input 'pip' field can be one of two cases:
        1) A List[str] describing the requirements. This is passed through.
        2) A string pointing to a local requirements file. In this case, the
           file contents will be read split into a list.
        3) A python dictionary that has three fields:
            a) packages (required, List[str]): a list of pip packages, it same as 1).
            b) pip_check (optional, bool): whether to enable pip check at the end of pip
               install, default to False.
            c) pip_version (optional, str): the version of pip, ray will spell
               the package name 'pip' in front of the `pip_version` to form the final
               requirement string, the syntax of a requirement specifier is defined in
               full in PEP 508.

    The returned parsed value will be a list of pip packages. If a Ray library
    (e.g. "ray[serve]") is specified, it will be deleted and replaced by its
    dependencies (e.g. "uvicorn", "requests").
    """
    assert pip is not None

    def _handle_local_pip_requirement_file(pip_file: str):
        pip_path = Path(pip_file)
        if not pip_path.is_file():
            raise ValueError(f"{pip_path} is not a valid file")
        return pip_path.read_text().strip().split("\n")

    result = None
    if sys.platform == "win32":
        logger.warning(
            "runtime environment support is experimental on Windows. "
            "If you run into issues please file a report at "
            "https://github.com/ray-project/ray/issues."
        )
    if isinstance(pip, str):
        # We have been given a path to a requirements.txt file.
        pip_list = _handle_local_pip_requirement_file(pip)
        result = dict(packages=pip_list, pip_check=False)
    elif isinstance(pip, list) and all(isinstance(dep, str) for dep in pip):
        result = dict(packages=pip, pip_check=False)
    elif isinstance(pip, dict):
        if set(pip.keys()) - {"packages", "pip_check", "pip_version"}:
            raise ValueError(
                "runtime_env['pip'] can only have these fields: "
                "packages, pip_check and pip_version, but got: "
                f"{list(pip.keys())}"
            )

        if "pip_check" in pip and not isinstance(pip["pip_check"], bool):
            raise TypeError(
                "runtime_env['pip']['pip_check'] must be of type bool, "
                f"got {type(pip['pip_check'])}"
            )
        if "pip_version" in pip:
            if not isinstance(pip["pip_version"], str):
                raise TypeError(
                    "runtime_env['pip']['pip_version'] must be of type str, "
                    f"got {type(pip['pip_version'])}"
                )
        result = pip.copy()
        result["pip_check"] = pip.get("pip_check", False)
        if "packages" not in pip:
            raise ValueError(
                f"runtime_env['pip'] must include field 'packages', but got {pip}"
            )
        elif isinstance(pip["packages"], str):
            result["packages"] = _handle_local_pip_requirement_file(pip["packages"])
        elif not isinstance(pip["packages"], list):
            raise ValueError(
                "runtime_env['pip']['packages'] must be of type str of list, "
                f"got: {type(pip['packages'])}"
            )
    else:
        raise TypeError(
            "runtime_env['pip'] must be of type str or " f"List[str], got {type(pip)}"
        )

    # Eliminate duplicates to prevent `pip install` from erroring. Use
    # OrderedDict to preserve the order of the list.  This makes the output
    # deterministic and easier to debug, because pip install can have
    # different behavior depending on the order of the input.
    result["packages"] = list(OrderedDict.fromkeys(result["packages"]))

    if len(result["packages"]) == 0:
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

    Args:
        env_vars: A dictionary of environment variables to set in the
            runtime environment.

    Returns:
        The validated env_vars dictionary, or None if it was empty.

    Raises:
        TypeError: If the env_vars is not a dictionary of strings. The error message
            will include the type of the invalid value.
    """
    assert env_vars is not None
    if len(env_vars) == 0:
        return None

    if not isinstance(env_vars, dict):
        raise TypeError(
            "runtime_env['env_vars'] must be of type "
            f"Dict[str, str], got {type(env_vars)}"
        )

    for key, val in env_vars.items():
        if not isinstance(key, str):
            raise TypeError(
                "runtime_env['env_vars'] must be of type "
                f"Dict[str, str], but the key {key} is of type {type(key)}"
            )
        if not isinstance(val, str):
            raise TypeError(
                "runtime_env['env_vars'] must be of type "
                f"Dict[str, str], but the value {val} is of type {type(val)}"
            )

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
