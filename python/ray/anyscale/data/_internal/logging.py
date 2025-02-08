import ray.data._internal.logging as ray_data_logging

RAY_TURBO_DEFAULT_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "ray": {
            "format": "%(asctime)s\t%(levelname)s %(filename)s:%(lineno)s -- %(message)s"  # noqa: E501
        },
        "ray_json": {"class": "ray._private.ray_logging.formatters.JSONFormatter"},
    },
    "filters": {
        "console_filter": {"()": "ray.data._internal.logging.HiddenRecordFilter"},
        "core_context_filter": {
            "()": "ray._private.ray_logging.filters.CoreContextFilter"
        },
    },
    "handlers": {
        "file": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray",
            "filename": "ray-data.log",
        },
        "file_json": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-data.log",
            "filters": ["core_context_filter"],
        },
        "console": {
            "class": "ray._private.log.PlainRayHandler",
            "formatter": "ray",
            "level": "INFO",
            "filters": ["console_filter"],
        },
        "error_file": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray",
            "filename": "ray-data-errors.log",
            "level": "ERROR",
        },
        "error_file_json": {
            "class": "ray.data._internal.logging.SessionFileHandler",
            "formatter": "ray_json",
            "filename": "ray-data-errors.log",
            "level": "ERROR",
            "filters": ["core_context_filter"],
        },
    },
    "loggers": {
        "ray.data": {
            "level": "DEBUG",
            "handlers": ["file", "console", "error_file"],
            "propagate": False,
        },
        "ray.anyscale.data": {
            "level": "DEBUG",
            "handlers": ["file", "console", "error_file"],
            "propagate": False,
        },
        "ray.anyscale.air._internal.autoscaling_coordinator": {
            "level": "DEBUG",
            "handlers": ["file", "console", "error_file"],
            "propagate": False,
        },
        "ray.air.util.tensor_extensions": {
            "level": "DEBUG",
            "handlers": ["file", "console", "error_file"],
            "propagate": False,
        },
    },
}

RAY_TURBO_DATA_LOG_HANDLER_JSON_SUBSTITUTIONS = {"error_file": "error_file_json"}


def configure_anyscale_logging() -> None:
    """Configure the Python logger named 'ray.data' for use within Anyscale.

    This function relies on the existing ray.data._internal.logging.configure_logging
    function, which is called here to setup ray logging behavior. Anyscale
    specific functionality is configured in this function..
    """
    # Override default config path with ray turbo config.
    ray_data_logging.DEFAULT_CONFIG = RAY_TURBO_DEFAULT_CONFIG

    # Override default substitutions with ray turbo substitutions
    ray_data_logging.RAY_DATA_LOG_HANDLER_JSON_SUBSTITUTIONS.update(
        RAY_TURBO_DATA_LOG_HANDLER_JSON_SUBSTITUTIONS
    )

    ray_data_logging.configure_logging()
