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
async def test_remote_code_model():
    """
    Tests that a remote code model fails to load when trust_remote_code=False
    and succeeds when trust_remote_code=True.

    If it loads successfully without remote code, the model should be changed to one
    that does require remote code.

    This helps avoid regressions for pickling issues for custom huggingface configs,
    since this custom code needs to be registered and imported across processes and workers.
    """

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
            pipeline_parallel_size=2,
            gpu_memory_utilization=0.92,
            dtype="auto",
            max_num_seqs=40,
            max_model_len=16384,
            enable_chunked_prefill=True,
            enable_prefix_caching=True,
        ),
    }

    # First part: Should fail with trust_remote_code=False
    base_config["engine_kwargs"]["trust_remote_code"] = False
    llm_config = LLMConfig(**base_config)
    app = build_openai_app({"llm_configs": [llm_config]})

    try:
        with pytest.raises(RuntimeError, match="Deploying application default failed"):
            serve.run(app, blocking=False)
    except AssertionError:
        # If pytest.raises fails, it means no RuntimeError was raised
        # Check if the app actually came up successfully
        try:
            default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
            if default_app.status == ApplicationStatus.RUNNING:
                pytest.fail(
                    "App deployed successfully without trust_remote_code=True. "
                    "This model may not actually require remote code. "
                    "Consider using a different model that requires remote code."
                )
        except (KeyError, AttributeError):
            pass
        finally:
            serve.shutdown()
        # Re-raise the original AssertionError if it wasn't a successful deployment
        raise

    # Second part: Should succeed with trust_remote_code=True
    base_config["engine_kwargs"]["trust_remote_code"] = True
    llm_config = LLMConfig(**base_config)
    app = build_openai_app({"llm_configs": [llm_config]})

    try:
        serve.run(app, blocking=False)

        def check_for_running_app():
            """Check if the application is running successfully."""
            try:
                default_app = serve.status().applications[SERVE_DEFAULT_APP_NAME]
                return default_app.status == ApplicationStatus.RUNNING
            except (KeyError, AttributeError):
                return False

        # Wait for the application to be running (timeout after 5 minutes)
        wait_for_condition(check_for_running_app, timeout=300)
    finally:
        # Clean up the deployment
        serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
