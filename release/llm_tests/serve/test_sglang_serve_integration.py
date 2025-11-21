import pytest
import sys
import requests
from ray.llm._internal.serve.core.configs.llm_config import LLMConfig
from ray import serve
from ray.llm._internal.serve.core.ingress.builder import build_sglang_openai_app

from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus
import time

def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False

@pytest.mark.parametrize("model_name", ["mistralai/Voxtral-Mini-3B-2507"])
def test_transcription_model(model_name):
    """
    Test that the transcription models can be loaded successfully.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
            model_source=model_name,
        ),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=4),
        ),
        engine_kwargs=dict(
            trust_remote_code=True,
            model_path=model_name,
            mem_fraction_static=0.9,
            context_length=2048,
        ),
    )
    app = build_sglang_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=180)
    url = "http://localhost:8000/v1/chat/completions"
    payload = {
        "model": target_model,
        "messages": [
            {"role": "user", "content": "Hello, are you working?"}
        ],
        "max_tokens": 10
    }

    # Send the request
    response = requests.post(url, json=payload)
    assert response.status_code == 200, f"Query failed: {response.text}"
    
    serve.shutdown()
    time.sleep(1)

@pytest.fixture
def remote_model_app(request):
    """
    Fixture that creates an app with a remote code model for testing.

    The remote_code parameter controls whether trust_remote_code is enabled.
    This helps avoid regressions for pickling issues for custom huggingface configs,
    since this custom code needs to be registered and imported across processes and workers.
    """
    remote_code = request.param

    base_config = {
        "model_loading_config": dict(
            model_id="hmellor/Ilama-3.2-1B",
            model_source="hmellor/Ilama-3.2-1B",
        ),
        "deployment_config": dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        "engine_kwargs": dict(
            trust_remote_code=remote_code,
            model_path="hmellor/Ilama-3.2-1B",
        ),
    }

    llm_config = LLMConfig(**base_config)
    app = build_sglang_openai_app({"llm_configs": [llm_config]})

    yield app

    # Cleanup
    serve.shutdown()
    time.sleep(1)


class TestRemoteCode:
    """Tests for remote code model loading behavior."""

    @pytest.mark.parametrize("remote_model_app", [False], indirect=True)
    def test_remote_code_failure(self, remote_model_app):
        """
        Tests that a remote code model fails to load when trust_remote_code=False.

        If it loads successfully without remote code, the fixture should be changed to one that does require remote code.
        """
        app = remote_model_app
        with pytest.raises(RuntimeError, match="Deploying application default failed"):
            serve.run(app, blocking=False)

        def check_for_failed_deployment():
            """Check if the application deployment has failed."""
            try:
                default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
                return default_app.status == ApplicationStatus.DEPLOY_FAILED
            except (KeyError, AttributeError):
                return False

        # Wait for either failure or success (timeout after 2 minutes)
        try:
            wait_for_condition(check_for_failed_deployment, timeout=120)
        except TimeoutError:
            # If deployment didn't fail, check if it succeeded
            if is_default_app_running():
                pytest.fail(
                    "App deployed successfully without trust_remote_code=True. "
                    "This model may not actually require remote code. "
                    "Consider using a different model that requires remote code."
                )
            else:
                pytest.fail("Deployment did not fail or succeed within timeout period.")

    @pytest.mark.parametrize("remote_model_app", [True], indirect=True)
    def test_remote_code_success(self, remote_model_app):
        """
        Tests that a remote code model succeeds to load when trust_remote_code=True.
        """
        app = remote_model_app

        serve.run(app, blocking=False)

        # Wait for the application to be running (timeout after 5 minutes)
        wait_for_condition(is_default_app_running, timeout=300)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
