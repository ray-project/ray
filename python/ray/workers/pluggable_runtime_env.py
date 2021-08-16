import contextlib
import json
import logging
import threading

logger = logging.getLogger(__name__)
thread_local_logger = threading.local()
thread_local_logger.logger = None  # default


def get_hook_logger():
    """Retrieve a logger to be used by the setup hook function. Logs from this
       logger will be streamed to driver.
    """
    thread_logger = thread_local_logger.logger
    if thread_logger is None:
        logger.warning(
            "Tried to receive the per job logger in runtime env agent but it "
            "hasn't been properly setup, default to dashboard_agent logger.")
        thread_logger = logger
    return thread_logger


@contextlib.contextmanager
def using_thread_local_logger(new_logger):
    """Configure the logger to be used by the setup hook function. This sets
       a logger to be retrieved by get_hook_logger function.
    """
    thread_local_logger.logger = new_logger
    yield
    thread_local_logger.logger = None


class RuntimeEnvContext:
    """A context used to describe the created runtime env."""

    def __init__(self, conda_env_name=None):
        self.conda_env_name = conda_env_name

    def serialize(self) -> str:
        # serialize the context to json string.
        return json.dumps(self.__dict__)

    @staticmethod
    def deserialize(json_string):
        return RuntimeEnvContext(**json.loads(json_string))
