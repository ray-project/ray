from typing import Optional

from ray._private.structured_logging.constants import LOG_MODE_DICT
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class LoggingConfig:
    def __init__(self, encoding: Optional[str] = "TEXT", log_level: str = "INFO"):
        """
        The class is used to store the Python logging configuration. It will be applied
        to the root loggers of the driver process and all Ray task and actor processes
        that belong to the this job.

        Examples:
            .. testcode::

                import ray
                import logging

                ray.init(
                    logging_config=ray.LoggingConfig(encoding="TEXT", log_level="INFO")
                )

                @ray.remote
                def f():
                    logger = logging.getLogger()
                    logger.info("This is a Ray task")

                obj_ref = f.remote()
                ray.get(obj_ref)

        Args:
            encoding: encoding is a string, and it should be one of the keys in
                LOG_MODE_DICT, which has the corresponding predefined logging
                configuration.
            log_level: The log level for the logging configuration.
        """
        if encoding not in LOG_MODE_DICT:
            raise ValueError(
                f"Invalid encoding type: {encoding}. "
                f"Valid encoding types are: {list(LOG_MODE_DICT.keys())}"
            )
        self.encoding = encoding
        self.log_level = log_level

    def get_dict_config(self) -> dict:
        """Get the logging configuration based on the encoding type.
        Returns:
            dict: The logging configuration.
        """
        return LOG_MODE_DICT[self.encoding](self.log_level)
