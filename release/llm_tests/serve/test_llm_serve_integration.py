import pytest
import sys

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from ray.llm._internal.serve.deployments.llm.vllm.vllm_loggers import (
    RayPrometheusStatLogger,
)
from vllm import AsyncEngineArgs

from vllm.v1.engine.async_llm import AsyncLLM
from vllm.sampling_params import SamplingParams
from ray._common.test_utils import wait_for_condition
from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
from ray.serve.schema import ApplicationStatus


@pytest.mark.asyncio(scope="function")
async def test_engine_metrics():
    """
    Test that the stat logger can be created successfully.
    Keeping this test small to focus on instantiating the
    derived class correctly.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",
        dtype="auto",
        disable_log_stats=False,
        enforce_eager=True,
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


@pytest.mark.asyncio(scope="function")
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
            model_id="deepseek",
            model_source="deepseek-ai/DeepSeek-V2-Lite",
        ),
        "runtime_env": dict(env_vars={"VLLM_USE_V1": "1"}),
        "deployment_config": dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        "engine_kwargs": dict(
            tensor_parallel_size=2,
            # TODO(lk-chen): Enable PP after
            # https://github.com/vllm-project/vllm/issues/20647 being fixed
            pipeline_parallel_size=1,
            gpu_memory_utilization=0.92,
            dtype="auto",
            max_num_seqs=40,
            max_model_len=8192,
            enable_chunked_prefill=True,
            enable_prefix_caching=True,
            trust_remote_code=remote_code,
            enforce_eager=True,
        ),
    }

    llm_config = LLMConfig(**base_config)
    app = build_openai_app({"llm_configs": [llm_config]})

    yield app

    # Cleanup
    serve.shutdown()


class TestRemoteCode:
    """Tests for remote code model loading behavior."""

    def _is_default_app_running(self):
        """Check if the default application is running successfully."""
        try:
            default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            return default_app.status == ApplicationStatus.RUNNING
        except (KeyError, AttributeError):
            return False

    @pytest.mark.parametrize("remote_model_app", [False], indirect=True)
    def test_remote_code_failure(self, remote_model_app):
        """
        Tests that a remote code model fails to load when trust_remote_code=False.

        If it loads successfully without remote code, the fixture should be changed to one
        that does require remote code.
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
            if self._is_default_app_running():
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
        wait_for_condition(self._is_default_app_running, timeout=300)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
