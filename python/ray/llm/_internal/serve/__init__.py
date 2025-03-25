from ray.llm._internal.serve.observability import setup_observability
from ray.llm._internal.common.observability.logging_utils import (
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)

# Set up observability
disable_vllm_custom_ops_logger_on_cpu_nodes()
setup_observability()


def _worker_process_setup_hook():
    """Noop setup hook used for ENABLE_WORKER_PROCESS_SETUP_HOOK
    (see python/ray/llm/_internal/serve/configs/constants.py)."""
    pass
