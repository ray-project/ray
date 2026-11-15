"""
This file serves as a documentation example and CI test.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking.
2. Docs example (between __prefix_aware_example_start/end__): Embedded in Sphinx docs via literalinclude.
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

# __prefix_aware_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.request_router import PrefixCacheAffinityRouter

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "qwen3.5-0.8b",
        "model_source": "Qwen/Qwen3.5-0.8B",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 4,
            "max_replicas": 4,
        },
        "request_router_config": {
            "request_router_class": PrefixCacheAffinityRouter,
            "request_router_kwargs": {
                "imbalanced_threshold": 5,  # More aggressive load balancing
                "match_rate_threshold": 0.15,  # Require 15% match rate
                "do_eviction": True,  # Enable memory management
                "eviction_threshold_chars": 500_000,
                "eviction_target_chars": 400_000,
                "eviction_interval_secs": 30,
            },
        },
    },
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __prefix_aware_example_end__

status = ApplicationStatus.NOT_STARTED
timeout_seconds = 180
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
