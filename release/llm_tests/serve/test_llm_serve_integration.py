import pytest
import sys

from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app
from vllm import AsyncEngineArgs

from vllm.v1.engine.async_llm import AsyncLLM
from vllm.v1.metrics.ray_wrappers import RayPrometheusStatLogger
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
async def test_engine_metrics_with_lora():
    """
    Test that the stat logger can be created successfully with LoRA configuration.
    This test validates LoRA-enabled engine initialization and basic functionality.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",  # Using smaller model for testing
        disable_log_stats=False,
        enforce_eager=True,
        enable_prefix_caching=True,
        max_model_len=512,
        max_lora_rank=64,
        enable_lora=True,
        max_loras=3,
        max_cpu_loras=5,
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"lora-request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


@pytest.mark.asyncio(scope="function")
async def test_engine_metrics_with_spec_decode():
    """
    Test that the stat logger can be created successfully with speculative decoding configuration.
    This test validates speculative decoding engine initialization and basic functionality.
    """

    engine_args = AsyncEngineArgs(
        model="Qwen/Qwen2.5-0.5B-Instruct",
        dtype="auto",
        disable_log_stats=False,
        enforce_eager=True,
        trust_remote_code=True,
        enable_prefix_caching=True,
        max_model_len=256,
        speculative_config={
            "method": "ngram",
            "num_speculative_tokens": 5,
            "prompt_lookup_max": 4,
        },
    )

    engine = AsyncLLM.from_engine_args(
        engine_args, stat_loggers=[RayPrometheusStatLogger]
    )

    for i, prompt in enumerate(["What is the capital of France?", "What is 2+2?"]):
        results = engine.generate(
            request_id=f"spec-request-id-{i}",
            prompt=prompt,
            sampling_params=SamplingParams(max_tokens=10),
        )

        async for _ in results:
            pass


def is_default_app_running():
    """Check if the default application is running successfully."""
    try:
        default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
        return default_app.status == ApplicationStatus.RUNNING
    except (KeyError, AttributeError):
        return False


@pytest.mark.parametrize("model_name", ["deepseek-ai/DeepSeek-V2-Lite"])
def test_deepseek_model(model_name):
    """
    Test that the deepseek model can be loaded successfully.
    """
    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id=model_name,
            model_source=model_name,
        ),
        runtime_env=dict(env_vars={"VLLM_USE_V1": "1"}),
        deployment_config=dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=2,
            pipeline_parallel_size=2,
            gpu_memory_utilization=0.92,
            dtype="auto",
            max_num_seqs=40,
            max_model_len=8192,
            enable_chunked_prefill=True,
            enable_prefix_caching=True,
            enforce_eager=True,
            trust_remote_code=True,
        ),
    )
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    wait_for_condition(is_default_app_running, timeout=300)


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
            model_id="hmellor/Ilama-3.2-1B",
            model_source="hmellor/Ilama-3.2-1B",
        ),
        "runtime_env": dict(env_vars={"VLLM_USE_V1": "1"}),
        "deployment_config": dict(
            autoscaling_config=dict(min_replicas=1, max_replicas=1),
        ),
        "engine_kwargs": dict(
            trust_remote_code=remote_code,
        ),
    }

    llm_config = LLMConfig(**base_config)
    app = build_openai_app({"llm_configs": [llm_config]})

    yield app

    # Cleanup
    serve.shutdown()


class TestRemoteCode:
    """Tests for remote code model loading behavior."""

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
