from ray.llm._internal.batch.observability.logging.setup import (
    setup_logging,
)
from ray.llm._internal.common.observability.logging_utils import (
    disable_datasets_logger,
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)
from ray.llm._internal.common.observability.telemetry_utils import Once

_setup_observability_once = Once()


def _setup_observability():
    setup_logging()
    disable_datasets_logger()
    disable_vllm_custom_ops_logger_on_cpu_nodes()


def setup_observability():
    _setup_observability_once.do_once(_setup_observability)


__all__ = ["setup_observability"]
