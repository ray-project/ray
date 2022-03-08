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


def _build_proto_pip_runtime_env(runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv):
    """Construct pip runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("pip"):
        if isinstance(runtime_env_dict["pip"], list):
            runtime_env.python_runtime_env.pip_runtime_env.config.packages.extend(
                runtime_env_dict["pip"]
            )
        else:
            runtime_env.python_runtime_env.pip_runtime_env.virtual_env_name = (
                runtime_env_dict["pip"]
            )


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


def _build_proto_conda_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct conda runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("conda"):
        if isinstance(runtime_env_dict["conda"], str):
            runtime_env.python_runtime_env.conda_runtime_env.conda_env_name = (
                runtime_env_dict["conda"]
            )
        else:
            runtime_env.python_runtime_env.conda_runtime_env.config = json.dumps(
                runtime_env_dict["conda"], sort_keys=True
            )


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


def _build_proto_container_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct container runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("container"):
        container = runtime_env_dict["container"]
        runtime_env.python_runtime_env.container_runtime_env.image = container.get(
            "image", ""
        )
        runtime_env.python_runtime_env.container_runtime_env.worker_path = (
            container.get("worker_path", "")
        )
        runtime_env.python_runtime_env.container_runtime_env.run_options.extend(
            container.get("run_options", [])
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


def _build_proto_plugin_runtime_env(
    runtime_env_dict: dict, runtime_env: ProtoRuntimeEnv
):
    """Construct plugin runtime env protobuf from runtime env dict."""
    if runtime_env_dict.get("plugins"):
        for class_path, plugin_field in runtime_env_dict["plugins"].items():
            plugin = runtime_env.python_runtime_env.plugin_runtime_env.plugins.add()
            plugin.class_path = class_path
            plugin.config = json.dumps(plugin_field, sort_keys=True)


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
        self._cached_pb = None
        self.__proto_runtime_env = None

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

    @classmethod
    def deserialize(cls, serialized_runtime_env: str) -> "RuntimeEnv":  # noqa: F821
        proto_runtime_env = json_format.Parse(serialized_runtime_env, ProtoRuntimeEnv())
        return cls.from_proto(proto_runtime_env)

    def serialize(self) -> str:

        return json.dumps(
            json.loads(json_format.MessageToJson(self._proto_runtime_env)),
            sort_keys=True,
        )

    def to_dict(self) -> Dict:
        return deepcopy(self)

    @property
    def _proto_runtime_env(self):
        if self.__proto_runtime_env:
            return self.__proto_runtime_env
        proto_runtime_env = ProtoRuntimeEnv()
        proto_runtime_env.working_dir = self.get("working_dir", "")
        if "working_dir" in self:
            proto_runtime_env.uris.working_dir_uri = self["working_dir"]
        if "py_modules" in self:
            proto_runtime_env.python_runtime_env.py_modules.extend(self["py_modules"])
            for uri in self["py_modules"]:
                proto_runtime_env.uris.py_modules_uris.append(uri)
        if "conda" in self:
            uri = get_conda_uri(self)
            if uri is not None:
                proto_runtime_env.uris.conda_uri = uri
        if "pip" in self:
            uri = get_pip_uri(self)
            if uri is not None:
                proto_runtime_env.uris.pip_uri = uri
        env_vars = self.get("env_vars", {})
        proto_runtime_env.env_vars.update(env_vars.items())
        if "_ray_release" in self:
            proto_runtime_env.extensions["_ray_release"] = str(self["_ray_release"])
        if "_ray_commit" in self:
            proto_runtime_env.extensions["_ray_commit"] = str(self["_ray_commit"])
        if "_inject_current_ray" in self:
            proto_runtime_env.extensions["_inject_current_ray"] = str(
                self["_inject_current_ray"]
            )
        _build_proto_pip_runtime_env(self, proto_runtime_env)
        _build_proto_conda_runtime_env(self, proto_runtime_env)
        _build_proto_container_runtime_env(self, proto_runtime_env)
        _build_proto_plugin_runtime_env(self, proto_runtime_env)

        self.__proto_runtime_env = proto_runtime_env
        return self.__proto_runtime_env

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
        uris = self._proto_runtime_env.uris
        if (
            uris.working_dir_uri
            or uris.py_modules_uris
            or uris.conda_uri
            or uris.pip_uri
            or uris.plugin_uris
        ):
            return True
        return False

    def working_dir_uri(self) -> str:
        return self._proto_runtime_env.uris.working_dir_uri

    def py_modules_uris(self) -> List[str]:
        return list(self._proto_runtime_env.uris.py_modules_uris)

    def conda_uri(self) -> str:
        return self._proto_runtime_env.uris.conda_uri

    def pip_uri(self) -> str:
        return self._proto_runtime_env.uris.pip_uri

    def plugin_uris(self) -> List[str]:
        return list(self._proto_runtime_env.uris.plugin_uris)

    def working_dir(self) -> str:
        return self._proto_runtime_env.working_dir

    def py_modules(self) -> List[str]:
        return list(self._proto_runtime_env.python_runtime_env.py_modules)

    def env_vars(self) -> Dict:
        return dict(self._proto_runtime_env.env_vars)

    def plugins(self) -> List[Tuple[str, str]]:
        result = list()
        for (
            plugin
        ) in self._proto_runtime_env.python_runtime_env.plugin_runtime_env.plugins:
            result.append((plugin.class_path, plugin.config))
        return result

    def has_conda(self) -> str:
        return self._proto_runtime_env.python_runtime_env.HasField("conda_runtime_env")

    def conda_env_name(self) -> str:
        if not self.has_conda():
            return None
        if not self._proto_runtime_env.python_runtime_env.conda_runtime_env.HasField(
            "conda_env_name"
        ):
            return None
        return (
            self._proto_runtime_env.python_runtime_env.conda_runtime_env.conda_env_name
        )

    def conda_config(self) -> str:
        if not self.has_conda():
            return None
        if not self._proto_runtime_env.python_runtime_env.conda_runtime_env.HasField(
            "config"
        ):
            return None
        return self._proto_runtime_env.python_runtime_env.conda_runtime_env.config

    def has_pip(self) -> bool:
        return self._proto_runtime_env.python_runtime_env.HasField("pip_runtime_env")

    def pip_packages(self) -> List:
        if not self.has_pip():
            return []
        return list(
            self._proto_runtime_env.python_runtime_env.pip_runtime_env.config.packages
        )

    def get_extension(self, key) -> str:
        return self._proto_runtime_env.extensions.get(key)

    def has_py_container(self) -> bool:
        return self._proto_runtime_env.python_runtime_env.HasField(
            "container_runtime_env"
        )

    def py_container_image(self) -> str:
        if not self.has_py_container():
            return None
        return self._proto_runtime_env.python_runtime_env.container_runtime_env.image

    def py_container_run_options(self) -> List:
        if not self.has_py_container():
            return None
        return list(
            self._proto_runtime_env.python_runtime_env.container_runtime_env.run_options
        )
