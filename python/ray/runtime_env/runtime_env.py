import json
import logging
import os
from copy import deepcopy
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import ray
from ray._private.ray_constants import DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS
from ray._private.runtime_env.conda import get_uri as get_conda_uri
from ray._private.runtime_env.pip import get_uri as get_pip_uri
from ray._private.runtime_env.plugin_schema_manager import RuntimeEnvPluginSchemaManager
from ray._private.runtime_env.uv import get_uri as get_uv_uri
from ray._private.runtime_env.validation import OPTION_TO_VALIDATION_FN
from ray._private.thirdparty.dacite import from_dict
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvConfig as ProtoRuntimeEnvConfig,
)
from ray.util.annotations import PublicAPI

logger = logging.getLogger(__name__)


@PublicAPI(stability="stable")
class RuntimeEnvConfig(dict):
    """Used to specify configuration options for a runtime environment.

    The config is not included when calculating the runtime_env hash,
    which means that two runtime_envs with the same options but different
    configs are considered the same for caching purposes.

    Args:
        setup_timeout_seconds: The timeout of runtime environment
            creation, timeout is in seconds. The value `-1` means disable
            timeout logic, except `-1`, `setup_timeout_seconds` cannot be
            less than or equal to 0. The default value of `setup_timeout_seconds`
            is 600 seconds.
        eager_install: Indicates whether to install the runtime environment
            on the cluster at `ray.init()` time, before the workers are leased.
            This flag is set to `True` by default.
    """

    known_fields: Set[str] = {"setup_timeout_seconds", "eager_install", "log_files"}

    _default_config: Dict = {
        "setup_timeout_seconds": DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS,
        "eager_install": True,
        "log_files": [],
    }

    def __init__(
        self,
        setup_timeout_seconds: int = DEFAULT_RUNTIME_ENV_TIMEOUT_SECONDS,
        eager_install: bool = True,
        log_files: Optional[List[str]] = None,
    ):
        super().__init__()
        if not isinstance(setup_timeout_seconds, int):
            raise TypeError(
                "setup_timeout_seconds must be of type int, "
                f"got: {type(setup_timeout_seconds)}"
            )
        elif setup_timeout_seconds <= 0 and setup_timeout_seconds != -1:
            raise ValueError(
                "setup_timeout_seconds must be greater than zero "
                f"or equals to -1, got: {setup_timeout_seconds}"
            )
        self["setup_timeout_seconds"] = setup_timeout_seconds

        if not isinstance(eager_install, bool):
            raise TypeError(
                f"eager_install must be a boolean. got {type(eager_install)}"
            )
        self["eager_install"] = eager_install

        if log_files is not None:
            if not isinstance(log_files, list):
                raise TypeError(
                    "log_files must be a list of strings or None, got "
                    f"{log_files} with type {type(log_files)}."
                )
            for file_name in log_files:
                if not isinstance(file_name, str):
                    raise TypeError("Each item in log_files must be a string.")
        else:
            log_files = self._default_config["log_files"]

        self["log_files"] = log_files

    @staticmethod
    def parse_and_validate_runtime_env_config(
        config: Union[Dict, "RuntimeEnvConfig"]
    ) -> "RuntimeEnvConfig":
        if isinstance(config, RuntimeEnvConfig):
            return config
        elif isinstance(config, Dict):
            unknown_fields = set(config.keys()) - RuntimeEnvConfig.known_fields
            if len(unknown_fields):
                logger.warning(
                    "The following unknown entries in the runtime_env_config "
                    f"dictionary will be ignored: {unknown_fields}."
                )
            config_dict = dict()
            for field in RuntimeEnvConfig.known_fields:
                if field in config:
                    config_dict[field] = config[field]
            return RuntimeEnvConfig(**config_dict)
        else:
            raise TypeError(
                "runtime_env['config'] must be of type dict or RuntimeEnvConfig, "
                f"got: {type(config)}"
            )

    @classmethod
    def default_config(cls):
        return RuntimeEnvConfig(**cls._default_config)

    def build_proto_runtime_env_config(self) -> ProtoRuntimeEnvConfig:
        runtime_env_config = ProtoRuntimeEnvConfig()
        runtime_env_config.setup_timeout_seconds = self["setup_timeout_seconds"]
        runtime_env_config.eager_install = self["eager_install"]
        if self["log_files"] is not None:
            runtime_env_config.log_files.extend(self["log_files"])
        return runtime_env_config

    @classmethod
    def from_proto(cls, runtime_env_config: ProtoRuntimeEnvConfig):
        setup_timeout_seconds = runtime_env_config.setup_timeout_seconds
        # Cause python class RuntimeEnvConfig has validate to avoid
        # setup_timeout_seconds equals zero, so setup_timeout_seconds
        # on RuntimeEnvConfig is zero means other Language(except python)
        # dosn't assign value to setup_timeout_seconds. So runtime_env_agent
        # assign the default value to setup_timeout_seconds.
        if setup_timeout_seconds == 0:
            setup_timeout_seconds = cls._default_config["setup_timeout_seconds"]
        return cls(
            setup_timeout_seconds=setup_timeout_seconds,
            eager_install=runtime_env_config.eager_install,
            log_files=list(runtime_env_config.log_files),
        )

    def to_dict(self) -> Dict:
        return dict(deepcopy(self))


# Due to circular reference, field config can only be assigned a value here
OPTION_TO_VALIDATION_FN[
    "config"
] = RuntimeEnvConfig.parse_and_validate_runtime_env_config


@PublicAPI
class RuntimeEnv(dict):
    """This class is used to define a runtime environment for a job, task,
    or actor.

    See :ref:`runtime-environments` for detailed documentation.

    This class can be used interchangeably with an unstructured dictionary
    in the relevant API calls.

    Can specify a runtime environment whole job, whether running a script
    directly on the cluster, using Ray Job submission, or using Ray Client:

    .. code-block:: python

        from ray.runtime_env import RuntimeEnv
        # Starting a single-node local Ray cluster
        ray.init(runtime_env=RuntimeEnv(...))

    .. code-block:: python

        from ray.runtime_env import RuntimeEnv
        # Connecting to remote cluster using Ray Client
        ray.init("ray://123.456.7.89:10001", runtime_env=RuntimeEnv(...))

    Can specify different runtime environments per-actor or per-task using
    ``.options()`` or the ``@ray.remote`` decorator:

    .. code-block:: python

        from ray.runtime_env import RuntimeEnv
        # Invoke a remote task that runs in a specified runtime environment.
        f.options(runtime_env=RuntimeEnv(...)).remote()

        # Instantiate an actor that runs in a specified runtime environment.
        actor = SomeClass.options(runtime_env=RuntimeEnv(...)).remote()

        # Specify a runtime environment in the task definition. Future invocations via
        # `g.remote()` use this runtime environment unless overridden by using
        # `.options()` as above.
        @ray.remote(runtime_env=RuntimeEnv(...))
        def g():
            pass

        # Specify a runtime environment in the actor definition. Future instantiations
        # via `MyClass.remote()` use this runtime environment unless overridden by
        # using `.options()` as above.
        @ray.remote(runtime_env=RuntimeEnv(...))
        class MyClass:
            pass

    Here are some examples of RuntimeEnv initialization:

    .. code-block:: python

        # Example for using conda
        RuntimeEnv(conda={
            "channels": ["defaults"], "dependencies": ["codecov"]})
        RuntimeEnv(conda="pytorch_p36")   # Found on DLAMIs

        # Example for using container
        RuntimeEnv(
            container={"image": "anyscale/ray-ml:nightly-py38-cpu",
            "run_options": ["--cap-drop SYS_ADMIN","--log-level=debug"]})

        # Example for set env_vars
        RuntimeEnv(env_vars={"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"})

        # Example for set pip
        RuntimeEnv(
            pip={"packages":["tensorflow", "requests"], "pip_check": False,
            "pip_version": "==22.0.2;python_version=='3.8.11'"})

        # Example for using image_uri
        RuntimeEnv(
            image_uri="rayproject/ray:2.39.0-py312-cu123")

    Args:
        py_modules: List of URIs (either in the GCS or external
            storage), each of which is a zip file that Ray unpacks and
            inserts into the PYTHONPATH of the workers.
        working_dir: URI (either in the GCS or external storage) of a zip
            file that Ray unpacks in the directory of each task/actor.
        pip: Either a list of pip packages, a string
            containing the path to a pip requirements.txt file, or a Python
            dictionary that has three fields: 1) ``packages`` (required, List[str]): a
            list of pip packages, 2) ``pip_check`` (optional, bool): whether enable
            pip check at the end of pip install, defaults to False.
            3) ``pip_version`` (optional, str): the version of pip, Ray prepends
            the package name "pip" in front of the ``pip_version`` to form the final
            requirement string, the syntax of a requirement specifier is defined in
            full in PEP 508.
        uv: Either a list of pip packages, or a Python dictionary that has one field:
            1) ``packages`` (required, List[str]).
        conda: Either the conda YAML config, the name of a
            local conda env (e.g., "pytorch_p36"), or the path to a conda
            environment.yaml file.
            Ray automatically injects the dependency into the conda
            env to ensure compatibility with the cluster Ray. Ray may automatically
            mangle the conda name to avoid conflicts between runtime envs.
            This field can't be specified at the same time as the 'pip' field.
            To use pip with conda, specify your pip dependencies within
            the conda YAML config:
            https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually
        container: Require a given (Docker) container image,
            The Ray worker process runs in a container with this image.
            This parameter only works alone, or with the ``config`` or
            ``env_vars`` parameters.
            The `run_options` list spec is here:
            https://docs.docker.com/engine/reference/run/
        env_vars: Environment variables to set.
        worker_process_setup_hook: (Experimental) The setup hook that's
            called after workers start and before Tasks and Actors are scheduled.
            A module name (string type) or callable (function) can be passed.
            When a module name is passed, Ray worker should be able to access the
            module name. When a callable is passed, callable should be serializable.
            When a runtime env is specified by job submission API,
            only a module name (string) is allowed.
        nsight: Dictionary mapping nsight profile option name to it's value.
        config: config for runtime environment. Either
            a dict or a RuntimeEnvConfig. Field: (1) setup_timeout_seconds, the
            timeout of runtime environment creation,  timeout is in seconds.
        image_uri: URI to a container image. The Ray worker process runs
            in a container with this image. This parameter only works alone,
            or with the ``config`` or ``env_vars`` parameters.
    """

    known_fields: Set[str] = {
        "py_modules",
        "java_jars",
        "working_dir",
        "conda",
        "pip",
        "uv",
        "container",
        "excludes",
        "env_vars",
        "_ray_release",
        "_ray_commit",
        "_inject_current_ray",
        "config",
        # TODO(SongGuyang): We add this because the test
        # `test_experimental_package_github` set a `docker`
        # field which is not supported. We should remove it
        # with the test.
        "docker",
        "worker_process_setup_hook",
        "_nsight",
        "mpi",
        "image_uri",
    }

    extensions_fields: Set[str] = {
        "_ray_release",
        "_ray_commit",
        "_inject_current_ray",
    }

    def __init__(
        self,
        *,
        py_modules: Optional[List[str]] = None,
        working_dir: Optional[str] = None,
        pip: Optional[List[str]] = None,
        conda: Optional[Union[Dict[str, str], str]] = None,
        container: Optional[Dict[str, str]] = None,
        env_vars: Optional[Dict[str, str]] = None,
        worker_process_setup_hook: Optional[Union[Callable, str]] = None,
        nsight: Optional[Union[str, Dict[str, str]]] = None,
        config: Optional[Union[Dict, RuntimeEnvConfig]] = None,
        _validate: bool = True,
        mpi: Optional[Dict] = None,
        image_uri: Optional[str] = None,
        uv: Optional[List[str]] = None,
        **kwargs,
    ):
        super().__init__()

        runtime_env = kwargs
        if py_modules is not None:
            runtime_env["py_modules"] = py_modules
        if working_dir is not None:
            runtime_env["working_dir"] = working_dir
        if pip is not None:
            runtime_env["pip"] = pip
        if uv is not None:
            runtime_env["uv"] = uv
        if conda is not None:
            runtime_env["conda"] = conda
        if nsight is not None:
            runtime_env["_nsight"] = nsight
        if container is not None:
            runtime_env["container"] = container
        if env_vars is not None:
            runtime_env["env_vars"] = env_vars
        if config is not None:
            runtime_env["config"] = config
        if worker_process_setup_hook is not None:
            runtime_env["worker_process_setup_hook"] = worker_process_setup_hook
        if mpi is not None:
            runtime_env["mpi"] = mpi
        if image_uri is not None:
            runtime_env["image_uri"] = image_uri
        if runtime_env.get("java_jars"):
            runtime_env["java_jars"] = runtime_env.get("java_jars")

        self.update(runtime_env)

        # Blindly trust that the runtime_env has already been validated.
        # This is dangerous and should only be used internally (e.g., on the
        # deserialization codepath.
        if not _validate:
            return

        if (self.get("conda") is not None) + (self.get("pip") is not None) + (
            self.get("uv") is not None
        ) > 1:
            raise ValueError(
                "The 'pip' field, 'uv' field, and 'conda' field of "
                "runtime_env cannot be specified at the same time.\n"
                f"specified pip field: {self.get('pip')}\n"
                f"specified conda field: {self.get('conda')}\n"
                f"specified uv field: {self.get('uv')}\n"
                "To use pip with conda, please only set the 'conda'"
                "field, and specify your pip dependencies within the conda YAML "
                "config dict: see https://conda.io/projects/conda/en/latest/"
                "user-guide/tasks/manage-environments.html"
                "#create-env-file-manually"
            )

        if self.get("container"):
            invalid_keys = set(runtime_env.keys()) - {"container", "config", "env_vars"}
            if len(invalid_keys):
                raise ValueError(
                    "The 'container' field currently cannot be used "
                    "together with other fields of runtime_env. "
                    f"Specified fields: {invalid_keys}"
                )

        if self.get("image_uri"):
            invalid_keys = set(runtime_env.keys()) - {"image_uri", "config", "env_vars"}
            if len(invalid_keys):
                raise ValueError(
                    "The 'image_uri' field currently cannot be used "
                    "together with other fields of runtime_env. "
                    f"Specified fields: {invalid_keys}"
                )

        for option, validate_fn in OPTION_TO_VALIDATION_FN.items():
            option_val = self.get(option)
            if option_val is not None:
                del self[option]
                self[option] = option_val

        if "_ray_commit" not in self:
            if self.get("pip") or self.get("conda"):
                self["_ray_commit"] = ray.__commit__

        # Used for testing wheels that have not yet been merged into master.
        # If this is set to True, then we do not inject Ray into the conda
        # or pip dependencies.
        if "_inject_current_ray" not in self:
            if "RAY_RUNTIME_ENV_LOCAL_DEV_MODE" in os.environ:
                self["_inject_current_ray"] = True

        # NOTE(architkulkarni): This allows worker caching code in C++ to check
        # if a runtime env is empty without deserializing it.  This is a catch-
        # all; for validated inputs we won't set the key if the value is None.
        if all(val is None for val in self.values()):
            self.clear()

    def __setitem__(self, key: str, value: Any) -> None:
        if is_dataclass(value):
            jsonable_type = asdict(value)
        else:
            jsonable_type = value
        RuntimeEnvPluginSchemaManager.validate(key, jsonable_type)
        res_value = jsonable_type
        if key in RuntimeEnv.known_fields and key in OPTION_TO_VALIDATION_FN:
            res_value = OPTION_TO_VALIDATION_FN[key](jsonable_type)
            if res_value is None:
                return
        return super().__setitem__(key, res_value)

    def set(self, name: str, value: Any) -> None:
        self.__setitem__(name, value)

    def get(self, name, default=None, data_class=None):
        if name not in self:
            return default
        if not data_class:
            return self.__getitem__(name)
        else:
            return from_dict(data_class=data_class, data=self.__getitem__(name))

    @classmethod
    def deserialize(cls, serialized_runtime_env: str) -> "RuntimeEnv":  # noqa: F821
        return cls(_validate=False, **json.loads(serialized_runtime_env))

    def serialize(self) -> str:
        # To ensure the accuracy of Proto, `__setitem__` can only guarantee the
        # accuracy of a certain field, not the overall accuracy
        runtime_env = type(self)(_validate=True, **self)
        return json.dumps(
            runtime_env,
            sort_keys=True,
        )

    def to_dict(self) -> Dict:
        runtime_env_dict = dict(deepcopy(self))

        # Replace strongly-typed RuntimeEnvConfig with a dict to allow the returned
        # dict to work properly as a field in a dataclass. Details in issue #26986
        if runtime_env_dict.get("config"):
            runtime_env_dict["config"] = runtime_env_dict["config"].to_dict()

        return runtime_env_dict

    def has_working_dir(self) -> bool:
        return self.get("working_dir") is not None

    def working_dir_uri(self) -> Optional[str]:
        return self.get("working_dir")

    def py_modules_uris(self) -> List[str]:
        if "py_modules" in self:
            return list(self["py_modules"])
        return []

    def conda_uri(self) -> Optional[str]:
        if "conda" in self:
            return get_conda_uri(self)
        return None

    def pip_uri(self) -> Optional[str]:
        if "pip" in self:
            return get_pip_uri(self)
        return None

    def uv_uri(self) -> Optional[str]:
        if "uv" in self:
            return get_uv_uri(self)
        return None

    def plugin_uris(self) -> List[str]:
        """Not implemented yet, always return a empty list"""
        return []

    def working_dir(self) -> str:
        return self.get("working_dir", "")

    def py_modules(self) -> List[str]:
        if "py_modules" in self:
            return list(self["py_modules"])
        return []

    def java_jars(self) -> List[str]:
        if "java_jars" in self:
            return list(self["java_jars"])
        return []

    def mpi(self) -> Optional[Union[str, Dict[str, str]]]:
        return self.get("mpi", None)

    def nsight(self) -> Optional[Union[str, Dict[str, str]]]:
        return self.get("_nsight", None)

    def env_vars(self) -> Dict:
        return self.get("env_vars", {})

    def has_conda(self) -> str:
        if self.get("conda"):
            return True
        return False

    def conda_env_name(self) -> str:
        if not self.has_conda() or not isinstance(self["conda"], str):
            return None
        return self["conda"]

    def conda_config(self) -> str:
        if not self.has_conda() or not isinstance(self["conda"], dict):
            return None
        return json.dumps(self["conda"], sort_keys=True)

    def has_pip(self) -> bool:
        if self.get("pip"):
            return True
        return False

    def has_uv(self) -> bool:
        if self.get("uv"):
            return True
        return False

    def virtualenv_name(self) -> Optional[str]:
        if not self.has_pip() or not isinstance(self["pip"], str):
            return None
        return self["pip"]

    def pip_config(self) -> Dict:
        if not self.has_pip() or isinstance(self["pip"], str):
            return {}
        # Parse and validate field pip on method `__setitem__`
        self["pip"] = self["pip"]
        return self["pip"]

    def uv_config(self) -> Dict:
        if not self.has_uv() or isinstance(self["uv"], str):
            return {}
        # Parse and validate field pip on method `__setitem__`
        self["uv"] = self["uv"]
        return self["uv"]

    def get_extension(self, key) -> Optional[str]:
        if key not in RuntimeEnv.extensions_fields:
            raise ValueError(
                f"Extension key must be one of {RuntimeEnv.extensions_fields}, "
                f"got: {key}"
            )
        return self.get(key)

    def has_py_container(self) -> bool:
        if self.get("container"):
            return True
        return False

    def py_container_image(self) -> Optional[str]:
        if not self.has_py_container():
            return None
        return self["container"].get("image", "")

    def py_container_worker_path(self) -> Optional[str]:
        if not self.has_py_container():
            return None
        return self["container"].get("worker_path", "")

    def py_container_run_options(self) -> List:
        if not self.has_py_container():
            return None
        return self["container"].get("run_options", [])

    def image_uri(self) -> Optional[str]:
        return self.get("image_uri")

    def plugins(self) -> List[Tuple[str, Any]]:
        result = list()
        for key, value in self.items():
            if key not in self.known_fields:
                result.append((key, value))
        return result


def _merge_runtime_env(
    parent: Optional[RuntimeEnv],
    child: Optional[RuntimeEnv],
    override: bool = False,
) -> Optional[RuntimeEnv]:
    """Merge the parent and child runtime environments.

    If override = True, the child's runtime env overrides the parent's
    runtime env in the event of a conflict.

    Merging happens per key (i.e., "conda", "pip", ...), but
    "env_vars" are merged per env var key.

    It returns None if Ray fails to merge runtime environments because
    of a conflict and `override = False`.

    Args:
        parent: Parent runtime env.
        child: Child runtime env.
        override: If True, the child's runtime env overrides
            conflicting fields.
    Returns:
        The merged runtime env's if Ray successfully merges them.
        None if the runtime env's conflict. Empty dict if
        parent and child are both None.
    """
    if parent is None:
        parent = {}
    if child is None:
        child = {}

    parent = deepcopy(parent)
    child = deepcopy(child)
    parent_env_vars = parent.pop("env_vars", {})
    child_env_vars = child.pop("env_vars", {})

    if not override:
        if set(parent.keys()).intersection(set(child.keys())):
            return None
        if set(parent_env_vars.keys()).intersection(set(child_env_vars.keys())):  # noqa
            return None

    parent.update(child)
    parent_env_vars.update(child_env_vars)
    if parent_env_vars:
        parent["env_vars"] = parent_env_vars

    return parent
