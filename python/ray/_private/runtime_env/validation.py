import logging
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Union

import yaml

from ray._private.path_utils import is_path
from ray._private.runtime_env.packaging import parse_path

logger = logging.getLogger(__name__)


def validate_path(path: str) -> None:
    """Parse the path to ensure it is well-formed and exists."""
    parse_path(path)


def validate_uri(uri: str):
    try:
        from ray._private.runtime_env.packaging import Protocol, parse_uri

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


def _handle_local_deps_requirement_file(requirements_file: str):
    """Read the given [requirements_file], and return all required dependencies."""
    requirements_path = Path(requirements_file)
    if not requirements_path.is_file():
        raise ValueError(f"{requirements_path} is not a valid file")
    return requirements_path.read_text().strip().split("\n")


def validate_py_modules_uris(py_modules_uris: List[str]) -> List[str]:
    """Parses and validates a 'py_modules' option.

    Expects py_modules to be a list of URIs.
    """
    if not isinstance(py_modules_uris, list):
        raise TypeError(
            "`py_modules` must be a list of strings, got " f"{type(py_modules_uris)}."
        )

    for module in py_modules_uris:

        if not isinstance(module, str):
            raise TypeError("`py_module` must be a string, got " f"{type(module)}.")

        validate_uri(module)


def parse_and_validate_py_modules(py_modules: List[str]) -> List[str]:
    """Parses and validates a 'py_modules' option.

    Expects py_modules to be a list of local paths or URIs.
    """
    if not isinstance(py_modules, list):
        raise TypeError(
            "`py_modules` must be a list of strings, got " f"{type(py_modules)}."
        )

    for module in py_modules:

        if not isinstance(module, str):
            raise TypeError("`py_module` must be a string, got " f"{type(module)}.")

        if is_path(module):
            validate_path(module)
        else:
            validate_uri(module)

    return py_modules


def validate_working_dir_uri(working_dir_uri: str) -> str:
    """Parses and validates a 'working_dir' option."""
    if not isinstance(working_dir_uri, str):
        raise TypeError(
            "`working_dir` must be a string, got " f"{type(working_dir_uri)}."
        )

    validate_uri(working_dir_uri)


def parse_and_validate_working_dir(working_dir: str) -> str:
    """Parses and validates a 'working_dir' option.

    This can be a URI or a path.
    """
    assert working_dir is not None

    if not isinstance(working_dir, str):
        raise TypeError("`working_dir` must be a string, got " f"{type(working_dir)}.")

    if is_path(working_dir):
        validate_path(working_dir)
    else:
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

    result = conda
    if isinstance(conda, str):
        file_path = Path(conda)
        if file_path.suffix in (".yaml", ".yml"):
            if not file_path.is_file():
                raise ValueError(f"Can't find conda YAML file {file_path}.")
            try:
                result = yaml.safe_load(file_path.read_text())
            except Exception as e:
                raise ValueError(f"Failed to read conda file {file_path}: {e}.")
        elif file_path.is_absolute():
            if not file_path.is_dir():
                raise ValueError(f"Can't find conda env directory {file_path}.")
            result = str(file_path)
    elif isinstance(conda, dict):
        result = conda
    else:
        raise TypeError(
            "runtime_env['conda'] must be of type str or " f"dict, got {type(conda)}."
        )

    return result


def parse_and_validate_uv(uv: Union[str, List[str], Dict]) -> Optional[Dict]:
    """Parses and validates a user-provided 'uv' option.

    The value of the input 'uv' field can be one of two cases:
        1) A List[str] describing the requirements. This is passed through.
           Example usage: ["tensorflow", "requests"]
        2) a string containing the path to a local pip “requirements.txt” file.
        3) A python dictionary that has one field:
            a) packages (required, List[str]): a list of uv packages, it same as 1).
            b) uv_check (optional, bool): whether to enable pip check at the end of uv
               install, default to False.
            c) uv_version (optional, str): user provides a specific uv to use; if
               unspecified, default version of uv will be used.
            d) uv_pip_install_options (optional, List[str]): user-provided options for
              `uv pip install` command, default to ["--no-cache"].

    The returned parsed value will be a list of packages. If a Ray library
    (e.g. "ray[serve]") is specified, it will be deleted and replaced by its
    dependencies (e.g. "uvicorn", "requests").
    """
    assert uv is not None
    if sys.platform == "win32":
        logger.warning(
            "runtime environment support is experimental on Windows. "
            "If you run into issues please file a report at "
            "https://github.com/ray-project/ray/issues."
        )

    result: str = ""
    if isinstance(uv, str):
        uv_list = _handle_local_deps_requirement_file(uv)
        result = dict(packages=uv_list, uv_check=False)
    elif isinstance(uv, list) and all(isinstance(dep, str) for dep in uv):
        result = dict(packages=uv, uv_check=False)
    elif isinstance(uv, dict):
        if set(uv.keys()) - {
            "packages",
            "uv_check",
            "uv_version",
            "uv_pip_install_options",
        }:
            raise ValueError(
                "runtime_env['uv'] can only have these fields: "
                "packages, uv_check, uv_version and uv_pip_install_options, but got: "
                f"{list(uv.keys())}"
            )
        if "packages" not in uv:
            raise ValueError(
                f"runtime_env['uv'] must include field 'packages', but got {uv}"
            )
        if "uv_check" in uv and not isinstance(uv["uv_check"], bool):
            raise TypeError(
                "runtime_env['uv']['uv_check'] must be of type bool, "
                f"got {type(uv['uv_check'])}"
            )
        if "uv_version" in uv and not isinstance(uv["uv_version"], str):
            raise TypeError(
                "runtime_env['uv']['uv_version'] must be of type str, "
                f"got {type(uv['uv_version'])}"
            )
        if "uv_pip_install_options" in uv:
            if not isinstance(uv["uv_pip_install_options"], list):
                raise TypeError(
                    "runtime_env['uv']['uv_pip_install_options'] must be of type "
                    f"list[str] got {type(uv['uv_pip_install_options'])}"
                )
            # Check each item in installation option.
            for idx, cur_opt in enumerate(uv["uv_pip_install_options"]):
                if not isinstance(cur_opt, str):
                    raise TypeError(
                        "runtime_env['uv']['uv_pip_install_options'] must be of type "
                        f"list[str] got {type(cur_opt)} for {idx}-th item."
                    )

        result = uv.copy()
        result["uv_check"] = uv.get("uv_check", False)
        result["uv_pip_install_options"] = uv.get(
            "uv_pip_install_options", ["--no-cache"]
        )
        if not isinstance(uv["packages"], list):
            raise ValueError(
                "runtime_env['uv']['packages'] must be of type list, "
                f"got: {type(uv['packages'])}"
            )
    else:
        raise TypeError(
            "runtime_env['uv'] must be of type " f"List[str], or dict, got {type(uv)}"
        )

    # Deduplicate packages for package lists.
    result["packages"] = list(OrderedDict.fromkeys(result["packages"]))

    if len(result["packages"]) == 0:
        result = None
    logger.debug(f"Rewrote runtime_env `uv` field from {uv} to {result}.")
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
            d) pip_install_options (optional, List[str]): user-provided options for
              `pip install` command, defaults to ["--disable-pip-version-check", "--no-cache-dir"].

    The returned parsed value will be a list of pip packages. If a Ray library
    (e.g. "ray[serve]") is specified, it will be deleted and replaced by its
    dependencies (e.g. "uvicorn", "requests").
    """
    assert pip is not None
    result = None

    if sys.platform == "win32":
        logger.warning(
            "runtime environment support is experimental on Windows. "
            "If you run into issues please file a report at "
            "https://github.com/ray-project/ray/issues."
        )
    if isinstance(pip, str):
        # We have been given a path to a requirements.txt file.
        pip_list = _handle_local_deps_requirement_file(pip)
        result = dict(
            packages=pip_list,
            pip_check=False,
        )
    elif isinstance(pip, list) and all(isinstance(dep, str) for dep in pip):
        result = dict(packages=pip, pip_check=False)
    elif isinstance(pip, dict):
        if set(pip.keys()) - {
            "packages",
            "pip_check",
            "pip_install_options",
            "pip_version",
        }:
            raise ValueError(
                "runtime_env['pip'] can only have these fields: "
                "packages, pip_check, pip_install_options and pip_version, but got: "
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
        if "pip_install_options" in pip:
            if not isinstance(pip["pip_install_options"], list):
                raise TypeError(
                    "runtime_env['pip']['pip_install_options'] must be of type "
                    f"list[str] got {type(pip['pip_install_options'])}"
                )
            # Check each item in installation option.
            for idx, cur_opt in enumerate(pip["pip_install_options"]):
                if not isinstance(cur_opt, str):
                    raise TypeError(
                        "runtime_env['pip']['pip_install_options'] must be of type "
                        f"list[str] got {type(cur_opt)} for {idx}-th item."
                    )

        result = pip.copy()
        # Contrary to pip_check, we do not insert the default value of pip_install_options.
        # This is to maintain backwards compatibility with ray==2.0.1
        result["pip_check"] = pip.get("pip_check", False)

        if "packages" not in pip:
            raise ValueError(
                f"runtime_env['pip'] must include field 'packages', but got {pip}"
            )
        elif isinstance(pip["packages"], str):
            result["packages"] = _handle_local_deps_requirement_file(pip["packages"])
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
    "uv": parse_and_validate_uv,
    "env_vars": parse_and_validate_env_vars,
    "container": parse_and_validate_container,
}

# RuntimeEnv can be created with local paths
# for these options. However, after the packages
# for these options have been uploaded to GCS,
# they must be URIs. These functions provide the ability
# to validate that these options only contain well-formed URIs.
OPTION_TO_NO_PATH_VALIDATION_FN = {
    "working_dir": validate_working_dir_uri,
    "py_modules": validate_py_modules_uris,
}
