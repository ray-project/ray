from abc import ABC, abstractmethod
from typing import Set

from ray._private.ray_logging import default_impl
from ray._private.ray_logging.constants import LOGRECORD_STANDARD_ATTRS
from ray._private.ray_logging.formatters import TextFormatter, JSONFormatter
from ray._private.ray_logging.filters import CoreContextFilter
from ray.util.annotations import PublicAPI

from dataclasses import dataclass, field

import logging


class LoggingConfigurator(ABC):
    @abstractmethod
    def get_supported_encodings(self) -> Set[str]:
        raise NotImplementedError

    @abstractmethod
    def configure(self, logging_config: "LoggingConfig"):
        raise NotImplementedError


class DefaultLoggingConfigurator(LoggingConfigurator):
    def __init__(self):
        self._encoding_to_formatter = {
            "TEXT": TextFormatter(),
            "JSON": JSONFormatter(),
        }

    def get_supported_encodings(self) -> Set[str]:
        return self._encoding_to_formatter.keys()

    def configure(self, logging_config: "LoggingConfig"):
        formatter = self._encoding_to_formatter[logging_config.encoding]
        formatter.set_additional_log_standard_attrs(
            logging_config.additional_log_standard_attrs
        )

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


_logging_configurator: LoggingConfigurator = default_impl.get_logging_configurator()


# Class defines the logging configurations for a Ray job.
# To add a new logging configuration: (1) add a new field to this class; (2) Update the
# logic in the __post_init__ method in this class to add the validation logic;
# (3) Update the configure method in the DefaultLoggingConfigurator
# class to use the new field.
@PublicAPI(stability="alpha")
@dataclass
class LoggingConfig:
    encoding: str = "TEXT"
    log_level: str = "INFO"
    # The list of valid attributes are defined as LOGRECORD_STANDARD_ATTRS in
    # constants.py.
    additional_log_standard_attrs: list = field(default_factory=list)

    def __post_init__(self):
        if self.encoding not in _logging_configurator.get_supported_encodings():
            raise ValueError(
                f"Invalid encoding type: {self.encoding}. "
                "Valid encoding types are: "
                f"{list(_logging_configurator.get_supported_encodings())}"
            )

        for attr in self.additional_log_standard_attrs:
            if attr not in LOGRECORD_STANDARD_ATTRS:
                raise ValueError(
                    f"Unknown python logging standard attribute: {attr}. "
                    "The valid attributes are: "
                    f"{LOGRECORD_STANDARD_ATTRS}"
                )

    def _configure_logging(self):
        """Set up the logging configuration for the current process."""
        _logging_configurator.configure(self)

    def _apply(self):
        """Set up the logging configuration."""
        self._configure_logging()


LoggingConfig.__doc__ = """
    Logging configuration for a Ray job. These configurations are used to set up the
    root logger of the driver process and all Ray tasks and actor processes that belong
    to the job.

    Examples: 1. Configure the logging to use TEXT encoding.
        .. testcode::

            import ray
            import logging

            ray.init(
                logging_config=ray.LoggingConfig(encoding="TEXT", log_level="INFO", additional_log_standard_attrs=['name'])
            )

            @ray.remote
            def f():
                logger = logging.getLogger(__name__)
                logger.info("This is a Ray task")

            ray.get(f.remote())
            ray.shutdown()

        .. testoutput::
            :options: +MOCK

            2025-02-12 12:25:16,836 INFO test-log-config.py:11 -- This is a Ray task name=__main__ job_id=01000000 worker_id=51188d9448be4664bf2ea26ac410b67acaaa970c4f31c5ad3ae776a5 node_id=f683dfbffe2c69984859bc19c26b77eaf3866c458884c49d115fdcd4 task_id=c8ef45ccd0112571ffffffffffffffffffffffff01000000 task_name=f task_func_name=test-log-config.f timestamp_ns=1739391916836884000

    2. Configure the logging to use JSON encoding.
        .. testcode::

            import ray
            import logging

            ray.init(
                logging_config=ray.LoggingConfig(encoding="JSON", log_level="INFO", additional_log_standard_attrs=['name'])
            )

            @ray.remote
            def f():
                logger = logging.getLogger(__name__)
                logger.info("This is a Ray task")

            ray.get(f.remote())
            ray.shutdown()

        .. testoutput::
            :options: +MOCK

            {"asctime": "2025-02-12 12:25:48,766", "levelname": "INFO", "message": "This is a Ray task", "filename": "test-log-config.py", "lineno": 11, "name": "__main__", "job_id": "01000000", "worker_id": "6d307578014873fcdada0fa22ea6d49e0fb1f78960e69d61dfe41f5a", "node_id": "69e3a5e68bdc7eb8ac9abb3155326ee3cc9fc63ea1be04d11c0d93c7", "task_id": "c8ef45ccd0112571ffffffffffffffffffffffff01000000", "task_name": "f", "task_func_name": "test-log-config.f", "timestamp_ns": 1739391948766949000}

    Args:
        encoding: Encoding type for the logs. The valid values are
            {list(_logging_configurator.get_supported_encodings())}
        log_level: Log level for the logs. Defaults to 'INFO'. You can set
            it to 'DEBUG' to receive more detailed debug logs.
        additional_log_standard_attrs: List of additional standard python logger attributes to
            include in the log. Defaults to an empty list. The list of already
            included standard attributes are: "asctime", "levelname", "message",
            "filename", "lineno", "exc_text". The list of valid attributes are specified
            here: http://docs.python.org/library/logging.html#logrecord-attributes
    """  # noqa: E501
