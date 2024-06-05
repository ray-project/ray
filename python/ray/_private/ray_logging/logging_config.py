from ray._private.ray_logging.constants import LOG_MODE_DICT
from ray.util.annotations import PublicAPI

from dataclasses import dataclass


@PublicAPI(stability="alpha")
@dataclass
class LoggingConfig:
    """
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
        encoding: Encoding type for the logs. The valid value is 'TEXT'
        log_level: Log level for the logs. Defaults to 'INFO'. You can set
            it to 'DEBUG' to receive more detailed debug logs.
    """  # noqa: E501

    encoding: str = "TEXT"
    log_level: str = "INFO"

    def __post_init__(self):
        if self.encoding not in LOG_MODE_DICT:
            raise ValueError(
                f"Invalid encoding type: {self.encoding}. "
                f"Valid encoding types are: {list(LOG_MODE_DICT.keys())}"
            )

    def _get_dict_config(self) -> dict:
        """Get the logging configuration based on the encoding type.
        Returns:
            dict: The logging configuration.
        """
        return LOG_MODE_DICT[self.encoding](self.log_level)
