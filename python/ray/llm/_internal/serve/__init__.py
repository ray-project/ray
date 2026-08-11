import os

from ray.llm._internal.common.observability.logging_utils import (
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)
from ray.llm._internal.serve.observability import setup_observability

# Set up observability
disable_vllm_custom_ops_logger_on_cpu_nodes()
setup_observability()


def _worker_process_setup_hook():
    """Apply narrowly scoped worker-process compatibility settings.

    ``RAY_SERVE_LLM_DISABLE_CUTEDSL`` is an escape hatch for installations
    with a partially compatible CUTLASS Python package.  vLLM's ll_bf16
    warmup otherwise treats the package as available and fails while importing
    its auxiliary ``quack`` module.  Set the cached availability bit before
    the worker initializes its kernels; normal installations are unchanged.
    """
    if os.environ.get("RAY_SERVE_LLM_DISABLE_CUTEDSL") == "1":
        try:
            from vllm.model_executor.kernels.linear.cute_dsl import ll_bf16
        except ImportError:
            return
        ll_bf16._cutedsl_available = False
