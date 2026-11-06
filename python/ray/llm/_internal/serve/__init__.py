from ray.llm._internal.common.observability.logging_utils import (
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)
from ray.llm._internal.serve.observability import setup_observability

# Set up observability
disable_vllm_custom_ops_logger_on_cpu_nodes()
setup_observability()


def _worker_process_setup_hook():
    """Setup hook used for ENABLE_WORKER_PROCESS_SETUP_HOOK
    (see python/ray/llm/_internal/serve/configs/constants.py).

    Must not import torch, directly or through vllm. The hook runs when the worker
    process starts, before Ray sets OMP_NUM_THREADS to the actor's CPUs, and torch
    sizes its thread pool when it is first imported: every vLLM worker would then
    run as many threads as the host has cores.
    """
    # TODO(kourosh): Remove with compile_cache_compat.
    from ray.llm._internal.serve.engines.vllm.patches import compile_cache_compat

    compile_cache_compat.install()
