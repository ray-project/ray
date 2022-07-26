import logging
import os
import json
from abc import ABC
from typing import List, Dict, Tuple, Any

from ray._private.runtime_env.context import RuntimeEnvContext
from ray._private.runtime_env.uri_cache import URICache
from ray._private.runtime_env.constants import (
    RAY_RUNTIME_ENV_PLUGINS_ENV_VAR,
    RAY_RUNTIME_ENV_PLUGIN_DEFAULT_PRIORITY,
    RAY_RUNTIME_ENV_CLASS_FIELD_NAME,
    RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME,
    RAY_RUNTIME_ENV_PLUGIN_MIN_PRIORITY,
    RAY_RUNTIME_ENV_PLUGIN_MAX_PRIORITY,
)
from ray.util.annotations import DeveloperAPI
from ray._private.utils import import_attr

default_logger = logging.getLogger(__name__)


@DeveloperAPI
class RuntimeEnvPlugin(ABC):
    """Abstract base class for runtime environment plugins."""

    name: str = None
    priority: int = RAY_RUNTIME_ENV_PLUGIN_DEFAULT_PRIORITY

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


class PluginSetupContext:
    def __init__(self, name: str, config: Any, class_instance: object):
        self.name = name
        self.config = config
        self.class_instance = class_instance


class RuntimeEnvPluginManager:
    """This manager is used to load plugins in runtime env agent."""

    class Context:
        def __init__(self, class_instance, priority):
            self.class_instance = class_instance
            self.priority = priority

    def __init__(self):
        self.plugins: Dict[str, RuntimeEnvPluginManager.Context] = {}
        plugin_config_str = os.environ.get(RAY_RUNTIME_ENV_PLUGINS_ENV_VAR)
        if plugin_config_str:
            plugin_configs = json.loads(plugin_config_str)
            self.load_plugins(plugin_configs)

    def load_plugins(self, plugin_configs: List[Dict]):
        """Load runtime env plugins"""
        for plugin_config in plugin_configs:
            if (
                not isinstance(plugin_config, dict)
                or RAY_RUNTIME_ENV_CLASS_FIELD_NAME not in plugin_config
            ):
                raise RuntimeError(
                    f"Invalid runtime env plugin config {plugin_config}, "
                    "it should be a object which contains the "
                    f"{RAY_RUNTIME_ENV_CLASS_FIELD_NAME} field."
                )
            plugin_class = import_attr(plugin_config[RAY_RUNTIME_ENV_CLASS_FIELD_NAME])
            if not issubclass(plugin_class, RuntimeEnvPlugin):
                raise RuntimeError(
                    f"Invalid runtime env plugin class {plugin_class}. "
                    "The plugin class must inherit "
                    "ray._private.runtime_env.plugin.RuntimeEnvPlugin."
                )
            if not plugin_class.name:
                raise RuntimeError(
                    f"No valid name in runtime env plugin {plugin_class}."
                )
            if plugin_class.name in self.plugins:
                raise RuntimeError(
                    f"The name of runtime env plugin {plugin_class} conflicts "
                    f"with {self.plugins[plugin_class.name]}.",
                )

            # The priority should be an integer between 0 and 100.
            # The default priority is 10. A smaller number indicates a
            # higher priority and the plugin will be set up first.
            if RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME in plugin_config:
                priority = plugin_config[RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME]
            else:
                priority = plugin_class.priority
            if (
                not isinstance(priority, int)
                or priority < RAY_RUNTIME_ENV_PLUGIN_MIN_PRIORITY
                or priority > RAY_RUNTIME_ENV_PLUGIN_MAX_PRIORITY
            ):
                raise RuntimeError(
                    f"Invalid runtime env priority {priority}, "
                    "it should be an integer between "
                    f"{RAY_RUNTIME_ENV_PLUGIN_MIN_PRIORITY} "
                    f"and {RAY_RUNTIME_ENV_PLUGIN_MAX_PRIORITY}."
                )

            self.plugins[plugin_class.name] = RuntimeEnvPluginManager.Context(
                plugin_class(), priority
            )

    def sorted_plugin_setup_contexts(
        self, inputs: List[Tuple[str, Any]]
    ) -> List[PluginSetupContext]:
        used_plugins = []
        for name, config in inputs:
            if name not in self.plugins:
                default_logger.error(
                    f"runtime_env field {name} is not recognized by "
                    "Ray and will be ignored.  In the future, unrecognized "
                    "fields in the runtime_env will raise an exception."
                )
                continue
            used_plugins.append(
                (
                    name,
                    config,
                    self.plugins[name].class_instance,
                    self.plugins[name].priority,
                )
            )
        sort_used_plugins = sorted(used_plugins, key=lambda x: x[3], reverse=False)
        return [
            PluginSetupContext(name, config, class_instance)
            for name, config, class_instance, _ in sort_used_plugins
        ]


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
