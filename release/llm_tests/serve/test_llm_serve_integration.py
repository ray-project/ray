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
    Tests that a remote code model can be loaded successfully. This is important
    to avoid regressions for pickling issues for custom huggingface configs.
    It may be a bit big/end-to-end, but it's good to have coverage for DeepSeek models
    in general.
    """

    llm_config = LLMConfig(
        model_loading_config=dict(
            model_id="deepseek",
            model_source="deepseek-ai/DeepSeek-V2-Lite",
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
            max_model_len=16384,
            enable_chunked_prefill=True,
            enable_prefix_caching=True,
            trust_remote_code=True,
        ),
    )

    app = build_openai_app({"llm_configs": [llm_config]})
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

    # Clean up the deployment
    serve.shutdown()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
