import os
import logging
from typing import Dict, List, Optional, Tuple, Any, Set, Union
import json
from google.protobuf import json_format
from copy import deepcopy

import ray
from ray.core.generated.runtime_env_common_pb2 import RuntimeEnv as ProtoRuntimeEnv
from ray._private.runtime_env.plugin import RuntimeEnvPlugin, encode_plugin_uri
from ray._private.runtime_env.validation import OPTION_TO_VALIDATION_FN
from ray._private.utils import import_attr
from ray._private.runtime_env.conda import (
    get_uri as get_conda_uri,
)

from ray._private.runtime_env.pip import get_uri as get_pip_uri
from ray.util.annotations import PublicAPI


logger = logging.getLogger(__name__)


def _parse_proto_pip_runtime_env(runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict):
    """Parse pip runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("pip_runtime_env"):
        if runtime_env.python_runtime_env.pip_runtime_env.HasField("config"):
            runtime_env_dict["pip"] = list(
                runtime_env.python_runtime_env.pip_runtime_env.config.packages
            )
        else:
            runtime_env_dict[
                "pip"
            ] = runtime_env.python_runtime_env.pip_runtime_env.virtual_env_name


def _parse_proto_conda_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse conda runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("conda_runtime_env"):
        if runtime_env.python_runtime_env.conda_runtime_env.HasField("conda_env_name"):
            runtime_env_dict[
                "conda"
            ] = runtime_env.python_runtime_env.conda_runtime_env.conda_env_name
        else:
            runtime_env_dict["conda"] = json.loads(
                runtime_env.python_runtime_env.conda_runtime_env.config
            )


def _parse_proto_container_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse container runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("container_runtime_env"):
        runtime_env_dict["container"] = dict()
        runtime_env_dict["container"][
            "image"
        ] = runtime_env.python_runtime_env.container_runtime_env.image
        runtime_env_dict["container"][
            "worker_path"
        ] = runtime_env.python_runtime_env.container_runtime_env.worker_path
        runtime_env_dict["container"]["run_options"] = list(
            runtime_env.python_runtime_env.container_runtime_env.run_options
        )


def _parse_proto_plugin_runtime_env(
    runtime_env: ProtoRuntimeEnv, runtime_env_dict: dict
):
    """Parse plugin runtime env protobuf to runtime env dict."""
    if runtime_env.python_runtime_env.HasField("plugin_runtime_env"):
        runtime_env_dict["plugins"] = dict()
        for plugin in runtime_env.python_runtime_env.plugin_runtime_env.plugins:
            runtime_env_dict["plugins"][plugin.class_path] = json.loads(plugin.config)


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
    ``.options()`` or the ``@ray.remote()`` decorator:

    .. code-block:: python

        from ray.runtime_env import RuntimeEnv
        # Invoke a remote task that will run in a specified runtime environment.
        f.options(runtime_env=RuntimeEnv(...)).remote()

        # Instantiate an actor that will run in a specified runtime environment.
        actor = SomeClass.options(runtime_env=RuntimeEnv(...)).remote()

        # Specify a runtime environment in the task definition. Future invocations via
        # `g.remote()` will use this runtime environment unless overridden by using
        # `.options()` as above.
        @ray.remote(runtime_env=RuntimeEnv(...))
        def g():
            pass

        # Specify a runtime environment in the actor definition. Future instantiations
        # via `MyClass.remote()` will use this runtime environment unless overridden by
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
            "worker_path": "/root/python/ray/workers/default_worker.py",
            "run_options": ["--cap-drop SYS_ADMIN","--log-level=debug"]})

        # Example for set env_vars
        RuntimeEnv(env_vars={"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"})

    Args:
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
        container (dict): Require a given (Docker) container image,
            The Ray worker process will run in a container with this image.
            The `worker_path` is the default_worker.py path.
            The `run_options` list spec is here:
            https://docs.docker.com/engine/reference/run/
        env_vars (dict): Environment variables to set.
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
        _validate: bool = True,
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
        if conda is not None:
            runtime_env["conda"] = conda
        if container is not None:
            runtime_env["container"] = container
        if env_vars is not None:
            runtime_env["env_vars"] = env_vars

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
                self[option] = option_val

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

        unknown_fields = set(runtime_env.keys()) - RuntimeEnv.known_fields
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

    def __setitem__(self, key: str, value: Any) -> None:
        if key not in RuntimeEnv.known_fields:
            logger.warning(
                "The following unknown entries in the runtime_env dictionary "
                f"will be ignored: {key}. If you intended to use "
                "them as plugins, they must be nested in the `plugins` field."
            )
            return
        res_value = value
        if key in OPTION_TO_VALIDATION_FN:
            res_value = OPTION_TO_VALIDATION_FN[key](value)
        return super().__setitem__(key, res_value)

    @classmethod
    def deserialize(cls, serialized_runtime_env: str) -> "RuntimeEnv":  # noqa: F821
        proto_runtime_env = json_format.Parse(serialized_runtime_env, ProtoRuntimeEnv())
        return cls.from_proto(proto_runtime_env)

    def serialize(self) -> str:
        # To ensure the accuracy of Proto, `__setitem__` can only guarantee the
        # accuracy of a certain field, not the overall accuracy
        runtime_env = type(self)(_validate=True, **self)
        proto_runtime_env = runtime_env.build_proto_runtime_env()
        return json.dumps(
            json.loads(json_format.MessageToJson(proto_runtime_env)),
            sort_keys=True,
        )

    def to_dict(self) -> Dict:
        return dict(deepcopy(self))

    def build_proto_runtime_env(self):
        proto_runtime_env = ProtoRuntimeEnv()

        # set working_dir
        proto_runtime_env.working_dir = self.working_dir()

        # set working_dir uri
        working_dir_uri = self.working_dir_uri()
        if working_dir_uri is not None:
            proto_runtime_env.uris.working_dir_uri = working_dir_uri

        # set py_modules
        py_modules_uris = self.py_modules_uris()
        if py_modules_uris:
            proto_runtime_env.python_runtime_env.py_modules.extend(py_modules_uris)
            # set py_modules uris
            proto_runtime_env.uris.py_modules_uris.extend(py_modules_uris)

        # set conda uri
        conda_uri = self.conda_uri()
        if conda_uri is not None:
            proto_runtime_env.uris.conda_uri = conda_uri

        # set pip uri
        pip_uri = self.pip_uri()
        if pip_uri is not None:
            proto_runtime_env.uris.pip_uri = pip_uri

        # set env_vars
        env_vars = self.env_vars()
        proto_runtime_env.env_vars.update(env_vars.items())

        # set extensions
        for extensions_field in RuntimeEnv.extensions_fields:
            if extensions_field in self:
                proto_runtime_env.extensions[extensions_field] = str(
                    self[extensions_field]
                )

        self._build_proto_pip_runtime_env(proto_runtime_env)
        self._build_proto_conda_runtime_env(proto_runtime_env)
        self._build_proto_container_runtime_env(proto_runtime_env)
        self._build_proto_plugin_runtime_env(proto_runtime_env)

        return proto_runtime_env

    @classmethod
    def from_proto(cls, proto_runtime_env: ProtoRuntimeEnv):
        initialize_dict: Dict[str, Any] = {}
        if proto_runtime_env.python_runtime_env.py_modules:
            initialize_dict["py_modules"] = list(
                proto_runtime_env.python_runtime_env.py_modules
            )
        if proto_runtime_env.working_dir:
            initialize_dict["working_dir"] = proto_runtime_env.working_dir
        if proto_runtime_env.env_vars:
            initialize_dict["env_vars"] = dict(proto_runtime_env.env_vars)
        if proto_runtime_env.extensions:
            initialize_dict.update(dict(proto_runtime_env.extensions))
        _parse_proto_pip_runtime_env(proto_runtime_env, initialize_dict)
        _parse_proto_conda_runtime_env(proto_runtime_env, initialize_dict)
        _parse_proto_container_runtime_env(proto_runtime_env, initialize_dict)
        _parse_proto_plugin_runtime_env(proto_runtime_env, initialize_dict)
        return cls(_validate=False, **initialize_dict)

    def has_uris(self) -> bool:
        if (
            self.working_dir_uri()
            or self.py_modules_uris()
            or self.conda_uri()
            or self.pip_uri()
            or self.plugin_uris()
        ):
            return True
        return False

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

    def plugin_uris(self) -> List[str]:
        """Not implemented yet, always return a empty list"""
        return []

    def working_dir(self) -> str:
        return self.get("working_dir", "")

    def py_modules(self) -> List[str]:
        if "py_modules" in self:
            return list(self["py_modules"])
        return []

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

    def pip_packages(self) -> List:
        if not self.has_pip() or not isinstance(self["pip"], list):
            return []
        return self["pip"]

    def virtualenv_name(self) -> Optional[str]:
        if not self.has_pip() or not isinstance(self["pip"], str):
            return None
        return self["pip"]

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

    def has_plugins(self) -> bool:
        if self.get("plugins"):
            return True
        return False

    def plugins(self) -> List[Tuple[str, str]]:
        result = list()
        if self.has_plugins():
            for class_path, plugin_field in self["plugins"].items():
                result.append((class_path, json.dumps(plugin_field, sort_keys=True)))
        return result

    def _build_proto_pip_runtime_env(self, runtime_env: ProtoRuntimeEnv):
        """Construct pip runtime env protobuf from runtime env dict."""
        if self.has_pip():
            pip_packages = self.pip_packages()
            virtualenv_name = self.virtualenv_name()
            # It is impossible for pip_packages and virtualenv_name
            # to be non-null at the same time
            if pip_packages:
                runtime_env.python_runtime_env.pip_runtime_env.config.packages.extend(
                    pip_packages
                )
            else:
                # It is impossible for virtualenv_name is None
                runtime_env.python_runtime_env.pip_runtime_env.virtual_env_name = (
                    virtualenv_name
                )

    def _build_proto_conda_runtime_env(self, runtime_env: ProtoRuntimeEnv):
        """Construct conda runtime env protobuf from runtime env dict."""
        if self.has_conda():
            conda_env_name = self.conda_env_name()
            conda_config = self.conda_config()
            # It is impossible for conda_env_name and conda_config
            # to be non-null at the same time
            if conda_env_name:
                runtime_env.python_runtime_env.conda_runtime_env.conda_env_name = (
                    conda_env_name
                )
            else:
                # It is impossible for conda_config is None
                runtime_env.python_runtime_env.conda_runtime_env.config = conda_config

    def _build_proto_container_runtime_env(self, runtime_env: ProtoRuntimeEnv):
        """Construct container runtime env protobuf from runtime env dict."""
        if self.has_py_container():
            runtime_env.python_runtime_env.container_runtime_env.image = (
                self.py_container_image()
            )
            runtime_env.python_runtime_env.container_runtime_env.worker_path = (
                self.py_container_worker_path()
            )
            runtime_env.python_runtime_env.container_runtime_env.run_options.extend(
                self.py_container_run_options()
            )

    def _build_proto_plugin_runtime_env(self, runtime_env: ProtoRuntimeEnv):
        """Construct plugin runtime env protobuf from runtime env dict."""
        if self.has_plugins():
            for class_path, plugin_field in self.plugins():
                plugin = runtime_env.python_runtime_env.plugin_runtime_env.plugins.add()
                plugin.class_path = class_path
                plugin.config = plugin_field

    def __getstate__(self):
        # When pickle serialization, exclude some fields
        # which can't be serialized by pickle
        return dict(**self)

    def __setstate__(self, state):
        for k, v in state.items():
            self[k] = v
        self.__proto_runtime_env = None
