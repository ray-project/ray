"""
This file serves as a documentation example and CI test for basic data parallel attention deployment.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __dp_basic_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve import llm

_original_serve_run = serve.run
_original_build_dp_openai_app = llm.build_dp_openai_app


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


def _testing_build_dp_openai_app(builder_config, **kwargs):
    """Removes accelerator requirements for testing"""
    if "llm_config" in builder_config:
        config = builder_config["llm_config"]
        if hasattr(config, "accelerator_type") and config.accelerator_type is not None:
            config.accelerator_type = None
    return _original_build_dp_openai_app(builder_config, **kwargs)


serve.run = _non_blocking_serve_run
llm.build_dp_openai_app = _testing_build_dp_openai_app

# __dp_basic_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_dp_openai_app

# Configure the model with data parallel settings
config = LLMConfig(
    model_loading_config={
        "model_id": "Qwen/Qwen2.5-0.5B-Instruct"
    },
    engine_kwargs={
        "data_parallel_size": 2,  # Number of DP replicas
        "tensor_parallel_size": 1,  # TP size per replica
    },
    experimental_configs={
        # This is a temporary required config. We will remove this in future versions.
        "dp_size_per_node": 2,  # DP replicas per node
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

