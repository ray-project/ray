"""
This file serves as a documentation example and CI test for data parallel + prefill-decode disaggregation.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __dp_pd_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve import llm
from ray.serve.llm.deployment import PDProxyServer
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

# Check if NIXL is available (required for NixlConnector)
try:
    import nixl  # noqa: F401
    NIXL_AVAILABLE = True
except ImportError:
    NIXL_AVAILABLE = False

if not NIXL_AVAILABLE:
    raise ImportError(
        "NIXL is required for this example but is not installed. "
        "Install it with: pip install nixl or uv pip install nixl"
    )

_original_serve_run = serve.run
_original_build_dp_deployment = llm.build_dp_deployment


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


def _testing_build_dp_deployment(llm_config, **kwargs):
    """Removes accelerator requirements for testing"""
    if llm_config.accelerator_type is not None:
        llm_config.accelerator_type = None
    return _original_build_dp_deployment(llm_config, **kwargs)


serve.run = _non_blocking_serve_run
llm.build_dp_deployment = _testing_build_dp_deployment

# __dp_pd_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_dp_deployment
from ray.serve.llm.deployment import PDProxyServer
from ray.serve.llm.ingress import OpenAiIngress, make_fastapi_ingress

# Configure prefill with data parallel attention
prefill_config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 2,  # 2 DP replicas for prefill
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    },
    experimental_configs={
        "dp_size_per_node": 2,
    },
)

# Configure decode with data parallel attention
decode_config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 2,  # 2 DP replicas for decode (adjusted for 4 GPU limit)
        "tensor_parallel_size": 1,
        "kv_transfer_config": {
            "kv_connector": "NixlConnector",
            "kv_role": "kv_both",
        }
    },
    experimental_configs={
        "dp_size_per_node": 2,
    },
)

# Build prefill and decode deployments with DP
prefill_deployment = build_dp_deployment(prefill_config, name_prefix="Prefill:")
decode_deployment = build_dp_deployment(decode_config, name_prefix="Decode:")

# Create PDProxyServer to coordinate between prefill and decode
proxy_options = PDProxyServer.get_deployment_options(prefill_config, decode_config)
proxy_deployment = serve.deployment(PDProxyServer).options(**proxy_options).bind(
    prefill_server=prefill_deployment,
    decode_server=decode_deployment,
)

# Create OpenAI-compatible ingress
ingress_options = OpenAiIngress.get_deployment_options([prefill_config, decode_config])
ingress_cls = make_fastapi_ingress(OpenAiIngress)
ingress_deployment = serve.deployment(ingress_cls).options(**ingress_options).bind(
    llm_deployments=[proxy_deployment]
)

# Deploy the application
serve.run(ingress_deployment, blocking=True)
# __dp_pd_example_end__

status = ApplicationStatus.NOT_STARTED
timeout_seconds = 300  # Longer timeout for DP+PD setup
start_time = time.time()

while (
    status != ApplicationStatus.RUNNING and time.time() - start_time < timeout_seconds
):
    status = serve.status().applications[SERVE_DEFAULT_APP_NAME].status

    if status in [ApplicationStatus.DEPLOY_FAILED, ApplicationStatus.UNHEALTHY]:
        raise AssertionError(f"Deployment failed with status: {status}")

    time.sleep(1)

if status != ApplicationStatus.RUNNING:
    raise AssertionError(
        f"Deployment failed to reach RUNNING status within {timeout_seconds}s. Current status: {status}"
    )

serve.shutdown()

