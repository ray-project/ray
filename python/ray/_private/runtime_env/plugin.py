import logging
import os
from abc import ABC
from typing import List

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.uri_cache import URICache
from ray._private.runtime_env.constants import RAY_RUNTIME_ENV_PLUGINS_ENV_VAR
from ray.util.annotations import DeveloperAPI
from ray._private.utils import import_attr

default_logger = logging.getLogger(__name__)


@DeveloperAPI
class RuntimeEnvPlugin(ABC):
    """Abstract base class for runtime environment plugins."""

    name: str = None

    @staticmethod
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

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        return None

    def create(
        self, uri: str, runtime_env: "RuntimeEnv", ctx: RuntimeEnvContext  # noqa: F821
    ) -> float:
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

    def modify_context(
        self,
        uris: List[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> None:
        """Modify context to change worker startup behavior.

        For example, you can use this to preprend "cd <dir>" command to worker
        startup, or add new environment variables.

        Args:
            uris(List[str]): a URIs used by this resource.
            runtime_env(RuntimeEnv): the runtime env protobuf.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.
        """
        return

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri(str): a URI uniquely describing this resource.
            ctx(RuntimeEnvContext): auxiliary information supplied by Ray.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0


class RuntimeEnvPluginManager:
    """This mananger is used to load plugins in runtime env agent."""

    def __init__(self):
        self.plugins = {}
        plugins_config = os.environ.get(RAY_RUNTIME_ENV_PLUGINS_ENV_VAR)
        if plugins_config:
            self.load_plugins(plugins_config.split(","))

    def load_plugins(self, plugin_classes: List[str]):
        """Load runtime env plugins"""
        for plugin_class_path in plugin_classes:
            plugin_class = import_attr(plugin_class_path)
            if not issubclass(plugin_class, RuntimeEnvPlugin):
                default_logger.warning(
                    "Invalid runtime env plugin class %s. "
                    "The plugin class must inherit "
                    "ray._private.runtime_env.plugin.RuntimeEnvPlugin.",
                    plugin_class,
                )
                continue
            if not plugin_class.name:
                default_logger.warning(
                    "No valid name in runtime env plugin %s", plugin_class
                )
                continue
            if plugin_class.name in self.plugins:
                default_logger.warning(
                    "The name of runtime env plugin %s conflicts with %s",
                    plugin_class,
                    self.plugins[plugin_class.name],
                )
                continue
            self.plugins[plugin_class.name] = plugin_class()

    def get_plugin(self, name: str):
        return self.plugins.get(name)


@DeveloperAPI
class PluginCacheManager:
    """Manages a plugin and a cache for its local resources."""

    def __init__(self, plugin: RuntimeEnvPlugin, uri_cache: URICache):
        self._plugin = plugin
        self._uri_cache = uri_cache

    async def create_if_needed(
        self,
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger = default_logger,
    ):
        uris = self._plugin.get_uris(runtime_env)
        for uri in uris:
            if uri not in self._uri_cache:
                logger.debug(f"Cache miss for URI {uri}.")
                size_bytes = await self._plugin.create(
                    uri, runtime_env, context, logger=logger
                )
                self._uri_cache.add(uri, size_bytes, logger=logger)
            else:
                logger.debug(f"Cache hit for URI {uri}.")
                self._uri_cache.mark_used(uri, logger=logger)

        self._plugin.modify_context(uris, runtime_env, context)
