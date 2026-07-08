"""Documentation example and CI test: custom vLLM model with Ray Serve LLM.

Structure:
1. Test-only setup: force serve.run non-blocking. The plugin is baked into the
   cluster image, and direct streaming is enabled through the cluster
   environment.
2. Docs example (between __custom_vllm_example_start/end__): embedded in the
   guide via literalinclude.
3. Test validation (deployment status polling + reward-head assertion + cleanup).
"""

import json
import time
import urllib.request

from ray import serve
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus

_original_serve_run = serve.run

def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing."""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)

serve.run = _non_blocking_serve_run

# __custom_vllm_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

# Serve Qwen3-0.6B with a scalar reward head from the `qwen3_reward_plugin`
# vLLM plugin (pip install it into your cluster image).
llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="qwen3-reward",
        model_source="Qwen/Qwen3-0.6B",
    ),
    # vLLM's entry point registers the architecture in the engine and worker
    # processes. Ray Serve LLM also resolves it while building the engine config.
    server_cls="qwen3_reward_plugin.serve_hook:RewardModelServer",
    engine_kwargs=dict(
        runner="pooling",
        hf_overrides=dict(
            architectures=["Qwen3CustomRewardModel"],
            num_labels=1,
            problem_type="regression",
        ),
        max_model_len=4096,
    ),
)

# Requires direct streaming so vLLM's native /classify route is exposed.
app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __custom_vllm_example_end__

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
        f"Deployment failed to reach RUNNING status within {timeout_seconds}s. "
        f"Current status: {status}"
    )

# Verify the reward head ran end to end: /classify returns a single scalar score
# per input (num_labels=1), read from data[0].probs.
body = json.dumps(
    {"model": "qwen3-reward", "input": "The capital of France is Paris."}
).encode()
request = urllib.request.Request(
    "http://localhost:8000/classify",
    data=body,
    headers={"Content-Type": "application/json"},
)
response = json.load(urllib.request.urlopen(request, timeout=60))
reward = response["data"][0]["probs"]
assert len(reward) == 1, f"Expected a scalar reward, got {len(reward)} values"

serve.shutdown()
