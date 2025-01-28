import logging
from typing import Set

import ray._private.ray_logging.logging_config as logging_config
from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter


class AnyscaleLoggingConfigurator(logging_config.LoggingConfigurator):
    def __init__(self):
        self._default_logging_configurator = logging_config.DefaultLoggingConfigurator()
        self._encoding_to_formatter = {
            "JSON": JSONFormatter(),
        }

    def get_supported_encodings(self) -> Set[str]:
        supported_encodings = set()
        supported_encodings.update(self._encoding_to_formatter.keys())
        supported_encodings.update(
            self._default_logging_configurator.get_supported_encodings()
        )
        return supported_encodings

    def configure(self, logging_config: "logging_config.LoggingConfig"):
        if logging_config.encoding in self._encoding_to_formatter:
            formatter = self._encoding_to_formatter[logging_config.encoding]
            core_context_filter = CoreContextFilter()
            handler = logging.StreamHandler()
            handler.setLevel(logging_config.log_level)
            handler.setFormatter(formatter)
            handler.addFilter(core_context_filter)

            root_logger = logging.getLogger()
            root_logger.setLevel(logging_config.log_level)
            root_logger.addHandler(handler)

            ray_logger = logging.getLogger("ray")
            ray_logger.setLevel(logging_config.log_level)
            # Remove all existing handlers added by `ray/__init__.py`.
            for h in ray_logger.handlers[:]:
                ray_logger.removeHandler(h)
            ray_logger.addHandler(handler)
            ray_logger.propagate = False
        else:
            self._default_logging_configurator.configure(logging_config)
