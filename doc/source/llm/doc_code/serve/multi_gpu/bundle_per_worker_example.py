# flake8: noqa
"""
This file serves as a documentation example and CI test for bundle_per_worker placement group config.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __bundle_per_worker_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve import llm

_original_serve_run = serve.run
_original_build_openai_app = llm.build_openai_app


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


def _testing_build_openai_app(config, **kwargs):
    """Removes accelerator requirements for testing"""
    llm_configs = config.get("llm_configs", [])
    for llm_config in llm_configs:
        if hasattr(llm_config, "accelerator_type") and llm_config.accelerator_type is not None:
            llm_config.accelerator_type = None
    return _original_build_openai_app(config, **kwargs)


serve.run = _non_blocking_serve_run
llm.build_openai_app = _testing_build_openai_app

# __bundle_per_worker_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Configure a model with bundle_per_worker for simple resource specification
# Ray automatically replicates this bundle based on tp * pp (2 bundles total)
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    engine_kwargs=dict(
        tensor_parallel_size=2,
        max_model_len=1024,
        max_num_seqs=32,
    ),
    # Simple: specify resources per worker, auto-replicated by tp*pp
    placement_group_config=dict(
        bundle_per_worker={"GPU": 1, "CPU": 1},
    ),
)

# Deploy the application
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __bundle_per_worker_example_end__

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
