"""
This file serves as a documentation example and CI test.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking and removes accelerator requirements for CI testing.
2. Docs example (between __transcription_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
import openai
import requests
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
    """Removes accelerator requirements for testing"""
    for config in llm_serving_args["llm_configs"]:
        config.accelerator_type = None

    return _original_build_openai_app(llm_serving_args)


serve.run = _non_blocking_serve_run
llm.build_openai_app = _testing_build_openai_app

# __transcription_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "voxtral-mini",
        "model_source": "mistralai/Voxtral-Mini-3B-2507",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 4,
        }
    },
    accelerator_type="A10G",
    # You can customize the engine arguments (e.g. vLLM engine kwargs)
    engine_kwargs={
        "tokenizer_mode": "mistral",
        "config_format": "mistral",
        "load_format": "mistral",
    },
    log_engine_metrics=True,
)

app = build_openai_app({"llm_configs": [llm_config]})
serve.run(app, blocking=True)
# __transcription_example_end__

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

response = requests.get("https://voiceage.com/wbsamples/in_stereo/Sports.wav")
with open("audio.wav", "wb") as f:
    f.write(response.content)

client = openai.OpenAI(base_url="http://localhost:8000/v1", api_key="fake-key")

with open("audio.wav", "rb") as f:
    try:
        response = client.audio.transcriptions.create(
            model="voxtral-mini",
            file=f,
            temperature=0.0,
            language="en",
        )
    except Exception as e:
        raise AssertionError(
            f"Error while querying models: {e}. Check the logs for more details."
        )

serve.shutdown()
