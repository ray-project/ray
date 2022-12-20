import logging
import os
import json
from abc import ABC
from typing import List, Dict, Optional, Any, Type

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
    def validate(runtime_env_dict: dict) -> None:
        """Validate user entry for this plugin.

        Args:
            runtime_env_dict: the user-supplied runtime environment dict.

        Raises:
            ValueError: if the validation fails.
        """
        pass

    def get_uris(self, runtime_env: "RuntimeEnv") -> List[str]:  # noqa: F821
        return []

    async def create(
        self,
        uri: Optional[str],
        runtime_env: "RuntimeEnv",  # noqa: F821
        context: RuntimeEnvContext,
        logger: logging.Logger,
    ) -> float:
        """Create and install the runtime environment.

        Gets called in the runtime env agent at install time. The URI can be
        used as a caching mechanism.

        Args:
            uri: A URI uniquely describing this resource.
            runtime_env: The RuntimeEnv object.
            context: auxiliary information supplied by Ray.
            logger: A logger to log messages during the context modification.

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
            uris: The URIs used by this resource.
            runtime_env: The RuntimeEnv object.
            context: Auxiliary information supplied by Ray.
            logger: A logger to log messages during the context modification.
        """
        return

    def delete_uri(self, uri: str, logger: logging.Logger) -> float:
        """Delete the the runtime environment given uri.

        Args:
            uri: a URI uniquely describing this resource.

        Returns:
            the amount of space reclaimed by the deletion.
        """
        return 0


class PluginSetupContext:
    def __init__(
        self,
        name: str,
        class_instance: RuntimeEnvPlugin,
        priority: int,
        uri_cache: URICache,
    ):
        self.name = name
        self.class_instance = class_instance
        self.priority = priority
        self.uri_cache = uri_cache


class RuntimeEnvPluginManager:
    """This manager is used to load plugins in runtime env agent."""

    def __init__(self):
        self.plugins: Dict[str, PluginSetupContext] = {}
        plugin_config_str = os.environ.get(RAY_RUNTIME_ENV_PLUGINS_ENV_VAR)
        if plugin_config_str:
            plugin_configs = json.loads(plugin_config_str)
            self.load_plugins(plugin_configs)

    def validate_plugin_class(self, plugin_class: Type[RuntimeEnvPlugin]) -> None:
        if not issubclass(plugin_class, RuntimeEnvPlugin):
            raise RuntimeError(
                f"Invalid runtime env plugin class {plugin_class}. "
                "The plugin class must inherit "
                "ray._private.runtime_env.plugin.RuntimeEnvPlugin."
            )
        if not plugin_class.name:
            raise RuntimeError(f"No valid name in runtime env plugin {plugin_class}.")
        if plugin_class.name in self.plugins:
            raise RuntimeError(
                f"The name of runtime env plugin {plugin_class} conflicts "
                f"with {self.plugins[plugin_class.name]}.",
            )

    def validate_priority(self, priority: Any) -> None:
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

    def load_plugins(self, plugin_configs: List[Dict]) -> None:
        """Load runtime env plugins and create URI caches for them."""
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
            self.validate_plugin_class(plugin_class)

            # The priority should be an integer between 0 and 100.
            # The default priority is 10. A smaller number indicates a
            # higher priority and the plugin will be set up first.
            if RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME in plugin_config:
                priority = plugin_config[RAY_RUNTIME_ENV_PRIORITY_FIELD_NAME]
            else:
                priority = plugin_class.priority
            self.validate_priority(priority)

            class_instance = plugin_class()
            self.plugins[plugin_class.name] = PluginSetupContext(
                plugin_class.name,
                class_instance,
                priority,
                self.create_uri_cache_for_plugin(class_instance),
            )

    def add_plugin(self, plugin: RuntimeEnvPlugin) -> None:
        """Add a plugin to the manager and create a URI cache for it.

        Args:
            plugin: The class instance of the plugin.
        """
        plugin_class = type(plugin)
        self.validate_plugin_class(plugin_class)
        self.validate_priority(plugin_class.priority)
        self.plugins[plugin_class.name] = PluginSetupContext(
            plugin_class.name,
            plugin,
            plugin_class.priority,
            self.create_uri_cache_for_plugin(plugin),
        )

    def create_uri_cache_for_plugin(self, plugin: RuntimeEnvPlugin) -> URICache:
        """Create a URI cache for a plugin.

        Args:
            plugin_name: The name of the plugin.

        Returns:
            The created URI cache for the plugin.
        """
        # Set the max size for the cache.  Defaults to 10 GB.
        cache_size_env_var = f"RAY_RUNTIME_ENV_{plugin.name}_CACHE_SIZE_GB".upper()
        cache_size_bytes = int(
            (1024**3) * float(os.environ.get(cache_size_env_var, 10))
        )
        return URICache(plugin.delete_uri, cache_size_bytes)

    def sorted_plugin_setup_contexts(self) -> List[PluginSetupContext]:
        """Get the sorted plugin setup contexts, sorted by increasing priority.

        Returns:
            The sorted plugin setup contexts.
        """
        return sorted(self.plugins.values(), key=lambda x: x.priority)


async def create_for_plugin_if_needed(
    runtime_env,
    plugin: RuntimeEnvPlugin,
    uri_cache: URICache,
    context: RuntimeEnvContext,
    logger: logging.Logger = default_logger,
):
    """Set up the environment using the plugin if not already set up and cached."""
    if plugin.name not in runtime_env or runtime_env[plugin.name] is None:
        return

    plugin.validate(runtime_env)

    uris = plugin.get_uris(runtime_env)

    if not uris:
        logger.debug(
            f"No URIs for runtime env plugin {plugin.name}; "
            "create always without checking the cache."
        )
        await plugin.create(None, runtime_env, context, logger=logger)

    for uri in uris:
        if uri not in uri_cache:
            logger.debug(f"Cache miss for URI {uri}.")
            size_bytes = await plugin.create(uri, runtime_env, context, logger=logger)
            uri_cache.add(uri, size_bytes, logger=logger)
        else:
            logger.debug(f"Cache hit for URI {uri}.")
            uri_cache.mark_used(uri, logger=logger)

    plugin.modify_context(uris, runtime_env, context, logger)
