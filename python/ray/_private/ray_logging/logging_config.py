from abc import ABC, abstractmethod
from typing import Set

from ray._private.ray_logging import default_impl
from ray._private.ray_logging.formatters import TextFormatter
from ray._private.ray_logging.filters import CoreContextFilter
from ray.util.annotations import PublicAPI

from dataclasses import dataclass

import logging


class LoggingConfigurator(ABC):
    @abstractmethod
    def get_supported_encodings(self) -> Set[str]:
        raise NotImplementedError

    @abstractmethod
    def configure_logging(self, encoding: str, log_level: str):
        raise NotImplementedError


class DefaultLoggingConfigurator(LoggingConfigurator):
    def __init__(self):
        self._encoding_to_formatter = {
            "TEXT": TextFormatter(),
        }

    def get_supported_encodings(self) -> Set[str]:
        return self._encoding_to_formatter.keys()

    def configure_logging(self, encoding: str, log_level: str):
        formatter = self._encoding_to_formatter[encoding]
        core_context_filter = CoreContextFilter()
        handler = logging.StreamHandler()
        handler.setLevel(log_level)
        handler.setFormatter(formatter)
        handler.addFilter(core_context_filter)

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        root_logger.addHandler(handler)

        ray_logger = logging.getLogger("ray")
        ray_logger.setLevel(log_level)
        # Remove all existing handlers added by `ray/__init__.py`.
        for h in ray_logger.handlers[:]:
            ray_logger.removeHandler(h)
        ray_logger.addHandler(handler)
        ray_logger.propagate = False


_logging_configurator: LoggingConfigurator = default_impl.get_logging_configurator()


@PublicAPI(stability="alpha")
@dataclass
class LoggingConfig:
    encoding: str = "TEXT"
    log_level: str = "INFO"

    def __post_init__(self):
        if self.encoding not in _logging_configurator.get_supported_encodings():
            raise ValueError(
                f"Invalid encoding type: {self.encoding}. "
                "Valid encoding types are: "
                f"{list(_logging_configurator.get_supported_encodings())}"
            )

    def _configure_logging(self):
        """Set up the logging configuration for the current process."""
        _logging_configurator.configure_logging(self.encoding, self.log_level)

    def _apply(self):
        """Set up the logging configuration."""
        self._configure_logging()


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
            {list(_logging_configurator.get_supported_encodings())}
        log_level: Log level for the logs. Defaults to 'INFO'. You can set
            it to 'DEBUG' to receive more detailed debug logs.
    """  # noqa: E501
