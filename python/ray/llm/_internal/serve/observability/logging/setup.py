import logging

import ray
from ray._private.ray_logging.filters import CoreContextFilter
from ray._private.ray_logging.formatters import JSONFormatter
from ray.serve._private.logging_utils import ServeContextFilter


def _configure_stdlib_logging():
    """Configures stdlib root logger to make sure stdlib loggers (created as
    `logging.getLogger(...)`) are using Ray's `JSONFormatter` with Core and Serve
     context filters.
    """

    handler = logging.StreamHandler()
    handler.addFilter(CoreContextFilter())
    handler.addFilter(ServeContextFilter())
    handler.setFormatter(JSONFormatter())

    root_logger = logging.getLogger()
    # NOTE: It's crucial we reset all the handlers of the root logger,
    #       to make sure that logs aren't emitted twice
    root_logger.handlers = []
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


def setup_logging():
    _configure_stdlib_logging()


def disable_vllm_custom_ops_logger_on_cpu_nodes():
    """This disables a log line in the "vllm._custom_ops" logger on CPU nodes.

    vllm._custom_ops is automatically imported when vllm is imported. It checks
    for CUDA binaries that don't exist in CPU-only nodes. This makes rayllm
    raise a scary-looking (but harmless) warning when imported on CPU nodes,
    such as when running the generate_config.py script or running the
    build-app task.
    """

    class SkipVLLMWarningFilter(logging.Filter):
        def filter(self, record: logging.LogRecord):
            """Only allow CRITICAL logs from the datasets/config.py file."""

            log_fragment = "Failed to import from vllm._C with"

            return log_fragment not in record.getMessage()

    if not ray.is_initialized() or len(ray.get_gpu_ids()) == 0:
        logging.getLogger("vllm._custom_ops").addFilter(SkipVLLMWarningFilter())


def disable_datasets_logger():
    """This disables "datasets" logs from its "config.py" file.

    Upon import, rayllm imports vllm which calls datasets. The datasets package
    emits a log from its config.py file.

    The file that emits this log uses the root "datasets" logger, so we use a
    filter to prevent logs from only the config.py file.
    """

    class SkipDatasetsConfigLogFilter(logging.Filter):
        def filter(self, record: logging.LogRecord):
            """Only allow CRITICAL logs from the datasets/config.py file."""
            return (
                record.levelno >= logging.CRITICAL
                or "datasets/config.py" not in record.pathname
            )

    logging.getLogger("datasets").addFilter(SkipDatasetsConfigLogFilter())
