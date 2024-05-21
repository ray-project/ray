from ray._private.structured_logging.formatters import TextFormatter
from ray._private.structured_logging.filters import CoreContextFilter

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


def get_log_config(encoding_type: str) -> dict:
    """Get the logging configuration based on the encoding type.

    Args:
        encoding_type: The encoding type of the logging configuration.

    Returns:
        dict: The logging configuration.
    """
    return LOG_MODE_DICT.get(encoding_type, {})
