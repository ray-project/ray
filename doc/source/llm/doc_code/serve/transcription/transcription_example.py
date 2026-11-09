"""
This file serves as a documentation example and CI test.

Structure:
1. Monkeypatch setup: Ensures serve.run is non-blocking.
2. Docs example (between __transcription_example_start/end__): Embedded in Sphinx docs via literalinclude.
3. Test validation (deployment status polling + cleanup)
"""

import time
import openai
import requests
from ray import serve
from ray.serve.schema import ApplicationStatus
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME

_original_serve_run = serve.run


def _non_blocking_serve_run(app, **kwargs):
    """Forces blocking=False for testing"""
    kwargs["blocking"] = False
    return _original_serve_run(app, **kwargs)


serve.run = _non_blocking_serve_run

# __transcription_example_start__
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app

llm_config = LLMConfig(
    model_loading_config={
        "model_id": "whisper-small",
        "model_source": "openai/whisper-small",
    },
    deployment_config={
        "autoscaling_config": {
            "min_replicas": 1,
            "max_replicas": 4,
        }
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
            model="whisper-small",
            file=f,
            temperature=0.0,
            language="en",
        )
    except Exception as e:
        raise AssertionError(
            f"Error while querying models: {e}. Check the logs for more details."
        )

serve.shutdown()
