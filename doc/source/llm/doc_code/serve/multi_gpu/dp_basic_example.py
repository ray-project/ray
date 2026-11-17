"""
This file serves as a documentation example and CI test for basic data parallel attention deployment.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking.
2. Docs example (between __dp_basic_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME

_original_serve_run = serve.run


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


serve.run = _non_blocking_serve_run

# __dp_basic_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_dp_openai_app

# Configure the model with data parallel settings
config = LLMConfig(
    model_loading_config={
        "model_id": "microsoft/Phi-tiny-MoE-instruct"
    },
    deployment_config={
        "num_replicas": 2
    },
    engine_kwargs={
        "data_parallel_size": 2,  # Number of DP replicas
        "tensor_parallel_size": 1,  # TP size per replica
        # Reduced for CI compatibility
        "max_model_len": 1024,
        "max_num_seqs": 32,
    },
)

app = build_dp_openai_app({
    "llm_config": config
})

serve.run(app, blocking=True)
# __dp_basic_example_end__

status = ApplicationStatus.NOT_STARTED
timeout_seconds = 300
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

