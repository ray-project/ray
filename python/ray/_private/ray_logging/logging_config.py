from abc import ABC, abstractmethod
from typing import Set

from ray._private.ray_logging import default_impl
from ray._private.ray_logging.constants import LOGRECORD_STANDARD_ATTRS
from ray._private.ray_logging.formatters import TextFormatter
from ray._private.ray_logging.filters import CoreContextFilter
from ray.util.annotations import PublicAPI, Deprecated

from dataclasses import asdict, dataclass, field

import logging


# The class used to pass the logging configs to the logging configurator. This class is
# used instead of the LoggingConfig class to avoid circular imports.
# The fields in this class should always match the fields in the LoggingConfig class.
@dataclass
class LoggingConfigData:
    encoding: str
    log_level: str
    additional_log_standard_attrs: list


class LoggingConfigurator(ABC):
    @abstractmethod
    def get_supported_encodings(self) -> Set[str]:
        raise NotImplementedError

    @Deprecated
    def configure_logging(self, encoding: str, log_level: str):
        raise NotImplementedError

    # Should be marked as @abstractmethod, but we can't do that because of backwards
    # compatibility.
    # Also, assuming the function will only be called inside LoggingConfig._apply() and
    # LoggingConfigData should have all fields in LoggingConfig set.
    def configure_logging_settings(self, logging_config: LoggingConfigData):
        self.configure_logging(logging_config.encoding, logging_config.log_level)


class DefaultLoggingConfigurator(LoggingConfigurator):
    def __init__(self):
        self._encoding_to_formatter = {
            "TEXT": TextFormatter(),
        }

    def get_supported_encodings(self) -> Set[str]:
        return self._encoding_to_formatter.keys()

    @Deprecated
    def configure_logging(self, encoding: str, log_level: str):
        self.configure_logging_settings(
            LoggingConfigData(
                encoding=encoding,
                log_level=log_level,
                additional_log_standard_attrs=[],
            )
        )

    def configure_logging_settings(self, logging_config: LoggingConfigData):
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
# To add a new logging configuration: (1) add a new field to this class; (2) add the
# same field to the LoggingConfigData class defined above; (3) Update the logic in the
# __post_init__ method in this class to add the validation logic; (4) Update the
# configure_logging_settings method in the DefaultLoggingConfigurator class to use
# the new field.
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
        _logging_configurator.configure_logging_settings(
            LoggingConfigData(asdict(self))
        )

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
                logging_config=ray.LoggingConfig(encoding="TEXT", log_level="INFO", additional_log_standard_attrs=['name'])
            )

            @ray.remote
            def f():
                logger = logging.getLogger(__name__)
                logger.info("This is a Ray task")

            ray.get(f.remote())

        .. testoutput::
            :options: +MOCK

            2024-06-03 07:53:50,815 INFO test.py:11 -- This is a Ray task name=__main__ job_id=01000000 worker_id=0dbbbd0f17d5343bbeee8228fa5ff675fe442445a1bc06ec899120a8 node_id=577706f1040ea8ebd76f7cf5a32338d79fe442e01455b9e7110cddfc task_id=c8ef45ccd0112571ffffffffffffffffffffffff01000000

    Args:
        encoding: Encoding type for the logs. The valid values are
            {list(_logging_configurator.get_supported_encodings())}
        log_level: Log level for the logs. Defaults to 'INFO'. You can set
            it to 'DEBUG' to receive more detailed debug logs.
        additional_log_standard_attrs: List of additional standard python logger attributes to 
            include in the log records. Defaults to an empty list. The list of already 
            included standard attributes are: "asctime", "levelname", "message", 
            "filename", "lineno", "exc_text". The list of valid attributes are specified
            here: http://docs.python.org/library/logging.html#logrecord-attributes
    """  # noqa: E501
