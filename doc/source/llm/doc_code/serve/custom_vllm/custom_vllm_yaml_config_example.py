"""Documentation example and CI test: custom vLLM model via a YAML config.

Structure:
1. Test-only setup: force serve.run non-blocking and strip accelerator
   requirements for CI. The plugin is baked into the cluster image, and direct
   streaming is enabled through the cluster environment.
2. Load the YAML config and deploy it with build_openai_app.
3. Test validation (deployment status polling + reward-head assertion + cleanup).
"""

import json
import os
import time
import urllib.request

import yaml

from ray import serve
from ray.serve import llm
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus

config_path = os.path.join(os.path.dirname(__file__), "custom_vllm_config.yaml")
with open(config_path, "r") as f:
    config_dict = yaml.safe_load(f)

llm_configs = config_dict["applications"][0]["args"]["llm_configs"]
app = llm.build_openai_app({"llm_configs": llm_configs})
serve.run(app, blocking=False)

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
