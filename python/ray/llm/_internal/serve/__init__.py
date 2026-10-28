import os

import ray
from ray.llm._internal.common.observability.logging_utils import (
    disable_vllm_custom_ops_logger_on_cpu_nodes,
)
from ray.llm._internal.serve.observability import setup_observability

# Set up observability
disable_vllm_custom_ops_logger_on_cpu_nodes()
setup_observability()


def _worker_process_setup_hook():
    """Setup hook used for ENABLE_WORKER_PROCESS_SETUP_HOOK
    (see python/ray/llm/_internal/serve/configs/constants.py)."""
    # ModelExpress (MX_SERVER_ADDRESS is set): each vLLM worker advertises its
    # own node for P2P, since a replica's workers can span nodes.
    if "MX_SERVER_ADDRESS" in os.environ and "MX_WORKER_HOST" not in os.environ:
        os.environ["MX_WORKER_HOST"] = ray.util.get_node_ip_address()

    # TODO(kourosh): Remove with compile_cache_compat.
    try:
        import vllm  # noqa: F401
    except ImportError:
        return
    from ray.llm._internal.serve.engines.vllm.patches import compile_cache_compat

    compile_cache_compat.apply()
