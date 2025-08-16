"""
This file serves as a documentation example and CI test for YAML config deployment.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Load YAML config and convert to Python using build_openai_app
3. Test validation (deployment status polling + cleanup)
"""

import time
import os
import yaml
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve import llm


config_path = os.path.join(os.path.dirname(__file__), "serve_my_llama_3_1_8B.yaml")
with open(config_path, "r") as f:
    config_dict = yaml.safe_load(f)

llm_configs = config_dict["applications"][0]["args"]["llm_configs"]
for config in llm_configs:
    config.pop("accelerator_type", None)
    # Use ungated llama instead
    config["model_loading_config"][
        "model_source"
    ] = "unsloth/Meta-Llama-3.1-8B-Instruct"
    config["engine_kwargs"]["hf_token"] = "PLACEHOLDER"

app = llm.build_openai_app({"llm_configs": llm_configs})
serve.run(app, blocking=False)

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
