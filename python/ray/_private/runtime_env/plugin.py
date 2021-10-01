from abc import ABC, abstractstaticmethod
import os
from typing import Dict, Iterable, Type

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.utils import import_attr
from ray.util.annotations import DeveloperAPI

RAY_PLUGIN_ENV_KEY_PREFIX = "RAY_RUNTIME_ENV_PLUGIN_"


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

    def create(uri: str, runtime_env_dict: dict,
               ctx: RuntimeEnvContext) -> float:
        """Create and install the runtime environment.

        Gets called in the runtime env agent at install time. The URI can be
        used as a caching mechanism.

        Args:
            uri(str): a URI uniquely describing this resource.
            runtime_env_dict(dict): the entire dictionary passed in by user.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the disk space taken up by this plugin installation for this
            environment. e.g. for working_dir, this downloads the files to the
            local node.
        """
        return 0

    def modify_context(uri: str, runtime_env_dict: dict,
                       ctx: RuntimeEnvContext) -> None:
        """Modify context to change worker startup behavior.

        For example, you can use this to preprend "cd <dir>" command to worker
        startup, or add new environment variables.

        Args:
            uri(str): a URI uniquely describing this resource.
            runtime_env_dict(dict): the entire dictionary passed in by user.
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


def load_plugins(
        runtime_env_keys: Iterable[str]) -> Dict[str, Type[RuntimeEnvPlugin]]:
    """Load plugins by parsing environment variables.

    Args:
        runtime_env_keys (Iterable[str]): the runtime_env keys supplied by the
          users. Only plugins that have a key matched will be loaded.
    Returns:
        A dictionary mapping key to plugin class.
    """

    loaded = dict()
    for key, value in os.environ.items():
        # This plugin is specified via environment variable.
        if key.startswith(RAY_PLUGIN_ENV_KEY_PREFIX):
            entry_key = key.replace(RAY_PLUGIN_ENV_KEY_PREFIX, "")

            # User has supplied a field matching this plugin.
            if entry_key in runtime_env_keys:
                plugin_class_path = value
                plugin_class: RuntimeEnvPlugin = import_attr(plugin_class_path)
                if not issubclass(plugin_class, RuntimeEnvPlugin):
                    # TODO(simon): move the class to public once ready.
                    raise TypeError(
                        f"{plugin_class_path} must be inherit from "
                        "ray._private.runtime_env.plugin.RuntimeEnvPlugin.")
                loaded[entry_key] = plugin_class
    return loaded
