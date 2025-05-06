"""Utilities for logging."""

import logging
import ray


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
