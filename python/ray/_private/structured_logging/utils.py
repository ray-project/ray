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
