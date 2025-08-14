"""
This file serves as a documentation example and CI test.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __llama_3_1_8B_example_start/end__): Embedded in Sphinx docs via literalinclude.
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


def _testing_build_openai_app(llm_serving_args):
    """Removes accelerator requirements for testing + use ungated model"""
    for config in llm_serving_args["llm_configs"]:
        config.accelerator_type = None
        # Use ungated llama instead
        config.model_loading_config[
            "model_source"
        ] = "unsloth/Meta-Llama-3.1-8B-Instruct"
        config.engine_kwargs["hf_token"] = "PLACEHOLDER"

    return _original_build_openai_app(llm_serving_args)


serve.run = _non_blocking_serve_run
llm.build_openai_app = _testing_build_openai_app

# __llama_3_1_8B_example_start__
# serve_my_llama_3_1_8B.py
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
import os

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id="my-llama-3.1-8B",
        # Or Qwen/Qwen2.5-7B for an ungated model
        model_source="meta-llama/Llama-3.1-8B-Instruct",
    ),
    accelerator_type="L4",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
        )
    ),
    engine_kwargs=dict(
        max_model_len=8192,
        ### If your model is not gated, you can skip `hf_token`
        # Share your Hugging Face Token to the vllm engine so it can access the gated Llama 3
        hf_token=os.environ["HF_TOKEN"],
    ),
)
app = build_openai_app({"llm_configs": [llm_config]})

serve.run(app, blocking=True)
# __llama_3_1_8B_example_end__

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
