from abc import ABC, abstractmethod
from typing import Set

from ray._private.ray_logging import default_impl
from ray._private.ray_logging.formatters import TextFormatter
from ray._private.ray_logging.filters import CoreContextFilter
from ray.util.annotations import PublicAPI

from dataclasses import dataclass

import logging
import time


class DictConfigProvider(ABC):
    @abstractmethod
    def get_supported_encodings(self) -> Set[str]:
        raise NotImplementedError

    @abstractmethod
    def get_dict_config(self, encoding: str, log_level: str) -> dict:
        raise NotImplementedError


class DefaultDictConfigProvider(DictConfigProvider):
    def __init__(self):
        self._dict_configs = {
            "TEXT": lambda log_level: {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "text": {
                        "()": (
                            f"{TextFormatter.__module__}."
                            f"{TextFormatter.__qualname__}"
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
                        "formatter": "text",
                        "filters": ["core_context"],
                    },
                },
                "root": {
                    "level": log_level,
                    "handlers": ["console"],
                },
            }
        }

    def get_supported_encodings(self) -> Set[str]:
        return self._dict_configs.keys()

    def get_dict_config(self, encoding: str, log_level: str) -> dict:
        return self._dict_configs[encoding](log_level)


_dict_config_provider: DictConfigProvider = default_impl.get_dict_config_provider()


@PublicAPI(stability="alpha")
@dataclass
class LoggingConfig:

    encoding: str = "TEXT"
    log_level: str = "INFO"

    def __post_init__(self):
        if self.encoding not in _dict_config_provider.get_supported_encodings():
            raise ValueError(
                f"Invalid encoding type: {self.encoding}. "
                "Valid encoding types are: "
                f"{list(_dict_config_provider.get_supported_encodings())}"
            )

    def _get_dict_config(self) -> dict:
        """Get the logging configuration based on the encoding type.
        Returns:
            dict: The logging configuration.
        """
        return _dict_config_provider.get_dict_config(self.encoding, self.log_level)

    def _setup_log_record_factory(self):
        old_factory = logging.getLogRecordFactory()

        def record_factory(*args, **kwargs):
            record = old_factory(*args, **kwargs)
            # Python logging module starts to use `time.time_ns()` to generate `created`
            # from Python 3.13 to avoid the precision loss caused by the float type.
            # Here, we generate the `created` for the LogRecord to support older Python
            # versions.
            ct = time.time_ns()
            record.created = ct / 1e9

            from ray._private.ray_logging.constants import LogKey

            record.__dict__[LogKey.TIMESTAMP_NS.value] = ct

            return record

        logging.setLogRecordFactory(record_factory)

    def _apply(self):
        """Set up both the LogRecord factory and the logging configuration."""
        self._setup_log_record_factory()
        logging.config.dictConfig(self._get_dict_config())


LoggingConfig.__doc__ = f"""
    Logging configuration for a Ray job. These configurations are used to set up the
    root logger of the driver process and all Ray tasks and actor processes that belong
    to the job.

    Examples:
        .. testcode::

            import ray
            import logging

            ray.init(
                logging_config=ray.LoggingConfig(encoding="TEXT", log_level="INFO")
            )

            @ray.remote
            def f():
                logger = logging.getLogger(__name__)
                logger.info("This is a Ray task")

            ray.get(f.remote())

        .. testoutput::
            :options: +MOCK

            2024-06-03 07:53:50,815 INFO test.py:11 -- This is a Ray task job_id=01000000 worker_id=0dbbbd0f17d5343bbeee8228fa5ff675fe442445a1bc06ec899120a8 node_id=577706f1040ea8ebd76f7cf5a32338d79fe442e01455b9e7110cddfc task_id=c8ef45ccd0112571ffffffffffffffffffffffff01000000

    Args:
        encoding: Encoding type for the logs. The valid values are
            {list(_dict_config_provider.get_supported_encodings())}
        log_level: Log level for the logs. Defaults to 'INFO'. You can set
            it to 'DEBUG' to receive more detailed debug logs.
    """  # noqa: E501
