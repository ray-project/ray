from ray._private.utils import import_attr
import logging
from typing import List

logger = logging.getLogger(__name__)


class RuntimeEnvPluginManager:
    """ This mananger is used to load plugins in runtime env agent.
    """
    plugins = {}

    @classmethod
    def load_plugins(cls, plugin_classes: List[str]):
        """ """
        for plugin_class_path in plugin_classes:
            plugin_class = import_attr(plugin_class_path)
            if not plugin_class.NAME:
                logger.error("No valid NAME in %s", plugin_class)
                continue
            if plugin_class.NAME in cls.plugins:
                logger.error(
                    "The plugin %s NAME conflicts with %s",
                    plugin_class,
                    cls.plugins[plugin_class.NAME],
                )
                continue
            cls.plugins[plugin_class.NAME] = plugin_class
