from ray._private.structured_logging.formatters import LogfmtFormatter
from ray._private.structured_logging.filters import CoreContextFilter

# A dictionary to map encoding types to their corresponding logging configurations.
LOG_MODE_DICT = {
    "TEXT": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "logfmt": {
                "()": LogfmtFormatter,
            },
        },
        "filters": {
            "core_context": {
                "()": CoreContextFilter,
            },
        },
        "handlers": {
            "console": {
                "level": "WARNING",
                "class": "logging.StreamHandler",
                "formatter": "logfmt",
                "filters": ["core_context"],
            },
        },
        "root": {
            "level": "WARNING",
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
