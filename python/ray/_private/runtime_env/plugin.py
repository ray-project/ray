from abc import ABC, abstractstaticmethod

from typing import Tuple
from ray.util.annotations import DeveloperAPI
from ray._private.runtime_env.context import RuntimeEnvContext
from ray.core.generated.common_pb2 import RuntimeEnv
import json


def build_proto_plugin_runtime_env(runtime_env_dict: dict,
                                   runtime_env: RuntimeEnv):
    """ Construct plugin runtime env protobuf from runtime env dict.
    """
    if runtime_env_dict.get("plugins"):
        for class_path, plugin_field in runtime_env_dict["plugins"].items():
            plugin = runtime_env.py_plugin_runtime_env.plugins.add()
            plugin.class_path = class_path
            plugin.config = json.dumps(plugin_field, sort_keys=True)


def parse_proto_plugin_runtime_env(runtime_env: RuntimeEnv,
                                   runtime_env_dict: dict):
    """ Parse plugin runtime env protobuf to runtime env dict.
    """
    if runtime_env.HasField("py_plugin_runtime_env"):
        for plugin in runtime_env.py_plugin_runtime_env.plugins:
            runtime_env_dict["plugins"][plugin.class_path] = dict(
                json.loads(plugin.config))


def encode_plugin_uri(plugin: str, uri: str) -> str:
    return plugin + "|" + uri


def decode_plugin_uri(plugin_uri: str) -> Tuple[str, str]:
    if "|" not in plugin_uri:
        raise ValueError(
            f"Plugin URI must be of the form 'plugin|uri', not {plugin_uri}")
    return tuple(plugin_uri.split("|", 2))


def decode_plugin_uri_from_pb(runtime_env: RuntimeEnv, plugin: str):
    for uri in runtime_env.uris:
        key, value = decode_plugin_uri(uri)
        if key == plugin:
            return value
    return None


@DeveloperAPI
class RuntimeEnvPlugin(ABC):
    @abstractstaticmethod
    def validate(runtime_env_dict: dict) -> str:
        """Validate user entry and returns a URI uniquely describing resource.

        This method will be called at ``f.options(runtime_env=...)`` or
        ``ray.init(runtime_env=...)`` time and it should check the runtime env
        dictionary for any errors. For example, it can raise "TypeError:
        expected string for "conda" field".

        Args:
            runtime_env_dict(dict): the entire dictionary passed in by user.

        Returns:
            uri(str): a URI uniquely describing this resource (e.g., a hash of
              the conda spec).
        """
        raise NotImplementedError()

    def create(uri: str, runtime_env: RuntimeEnv,
               ctx: RuntimeEnvContext) -> float:
        """Create and install the runtime environment.

        Gets called in the runtime env agent at install time. The URI can be
        used as a caching mechanism.

        Args:
            uri(str): a URI uniquely describing this resource.
            runtime_env(RuntimeEnv): the runtime env protobuf.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the disk space taken up by this plugin installation for this
            environment. e.g. for working_dir, this downloads the files to the
            local node.
        """
        return 0

    def modify_context(uri: str, runtime_env: RuntimeEnv,
                       ctx: RuntimeEnvContext) -> None:
        """Modify context to change worker startup behavior.

        For example, you can use this to preprend "cd <dir>" command to worker
        startup, or add new environment variables.

        Args:
            uri(str): a URI uniquely describing this resource.
            runtime_env(RuntimeEnv): the runtime env protobuf.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.
        """
        return

    def delete(uri: str, ctx: RuntimeEnvContext) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri(str): a URI uniquely describing this resource.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0
