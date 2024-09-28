from typing import Set

import ray._private.ray_logging.logging_config as logging_config
from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter


class AnyscaleDictConfigProvider(logging_config.DictConfigProvider):
    def __init__(self):
        self._default_dict_config_provider = logging_config.DefaultDictConfigProvider()
        self._dict_configs = {
            "JSON": lambda log_level: {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "json": {
                        "()": (
                            f"{JSONFormatter.__module__}."
                            f"{JSONFormatter.__qualname__}"
                        ),
                    },
                },
                "filters": {
                    "core_context": {
                        "()": (
                            f"{CoreContextFilter.__module__}."
                            f"{CoreContextFilter.__qualname__}"
                        ),
                    },
                },
                "handlers": {
                    "console": {
                        "level": log_level,
                        "class": "logging.StreamHandler",
                        "formatter": "json",
                        "filters": ["core_context"],
                    },
                },
                "root": {
                    "level": log_level,
                    "handlers": ["console"],
                },
                "loggers": {
                    "ray": {
                        "level": log_level,
                        "handlers": ["console"],
                        "propagate": False,
                    }
                },
            },
        }

    def get_supported_encodings(self) -> Set[str]:
        supported_encodings = set()
        supported_encodings.update(self._dict_configs.keys())
        supported_encodings.update(
            self._default_dict_config_provider.get_supported_encodings()
        )
        return supported_encodings

    def get_dict_config(self, encoding: str, log_level: str) -> dict:
        if encoding in self._dict_configs:
            return self._dict_configs[encoding](log_level)
        return self._default_dict_config_provider.get_dict_config(encoding, log_level)
