import logging
from pathlib import Path
import sys
from typing import Dict, List, Optional, Union
from pkg_resources import Requirement
from collections import OrderedDict
import yaml

from ray._private.runtime_env.conda import _resolve_install_from_source_ray_extras

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
    """Remove Ray and replace Ray libraries with their dependencies.

    The `pip` field of runtime_env installs packages into the current
    environment, inheriting the existing environment.  If users want to
    use Ray libraries like `ray[serve]` in their job, they must include
    `ray[serve]` in their `runtime_env` `pip` field.  However, without this
    function, the Ray installed at runtime would take precedence over the
    Ray that exists in the cluster, which would lead to version mismatch
    issues.

    To work around this, this function deletes Ray from the input `pip_list`
    if it's specified without any libraries (e.g. "ray" or "ray>1.4"). If
    a Ray library is specified (e.g. "ray[serve]"), it is replaced by
    its dependencies (e.g. "uvicorn", ...).

    """
    result = []
    for specifier in pip_list:
        try:
            requirement = Requirement.parse(specifier)
        except Exception:
            # Some lines in a pip_list might not be requirements but
            # rather options for `pip`; e.g. `--extra-index-url MY_INDEX`.
            # Requirement.parse would raise an InvalidRequirement in this
            # case.  Since we are only interested in lines specifying Ray
            # or its libraries, we should just skip this line.
            result.append(specifier)
            continue
        package_name = requirement.name
        if package_name == "ray":
            libraries = requirement.extras  # e.g. ("serve", "tune")
            if libraries == ():
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
