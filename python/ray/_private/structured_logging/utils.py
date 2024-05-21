from ray._private.structured_logging.formatters import TextFormatter
from ray._private.structured_logging.filters import CoreContextFilter
from typing import Union

# A dictionary to map encoding types to their corresponding logging configurations.
LOG_MODE_DICT = {
    "TEXT": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "text": {
                "()": TextFormatter,
            },
        },
        "filters": {
            "core_context": {
                "()": CoreContextFilter,
            },
        },
        "handlers": {
            "console": {
                "level": "INFO",
                "class": "logging.StreamHandler",
                "formatter": "text",
                "filters": ["core_context"],
            },
        },
        "root": {
            "level": "INFO",
            "handlers": ["console"],
        },
    },
}


class LoggingConfig:
    def __init__(self, log_config: Union[dict, str]):
        if isinstance(log_config, str):
            assert log_config in LOG_MODE_DICT, (
                f"Invalid encoding type: {log_config}. "
                f"Valid encoding types are: {list(LOG_MODE_DICT.keys())}"
            )
            self.log_config_str = log_config
        else:
            self.log_config_dict = log_config

    def get_log_config(self) -> dict:
        """Get the logging configuration based on the encoding type.
        Returns:
            dict: The logging configuration.
        """
        if hasattr(self, "log_config_dict"):
            return self.log_config_dict
        else:
            return LOG_MODE_DICT.get(self.log_config_str, {})
