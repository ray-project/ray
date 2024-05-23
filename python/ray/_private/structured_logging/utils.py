from ray._private.structured_logging.formatters import TextFormatter
from ray._private.structured_logging.filters import CoreContextFilter
from typing import Union

LOG_MODE_DICT = {
    "TEXT": lambda log_level: {
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
                "level": log_level,
                "class": "logging.StreamHandler",
                "formatter": "text",
                "filters": ["core_context"],
            },
        },
        "root": {
            "level": log_level,
            "handlers": ["console"],
        },
    },
}


class LoggingConfig:
    def __init__(self, log_config: Union[dict, str] = "TEXT", log_level: str = "INFO"):
        self.log_config = log_config
        self.log_level = log_level

    def get_dict_config(self) -> dict:
        """Get the logging configuration based on the encoding type.
        Returns:
            dict: The logging configuration.
        """
        if isinstance(self.log_config, str):
            if self.log_config not in LOG_MODE_DICT:
                raise ValueError(
                    f"Invalid encoding type: {self.log_config}. "
                    f"Valid encoding types are: {list(LOG_MODE_DICT.keys())}"
                )
            return LOG_MODE_DICT[self.log_config](self.log_level)
        return self.log_config
