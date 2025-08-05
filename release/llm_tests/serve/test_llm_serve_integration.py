import pytest
import sys

from ray.llm._internal.serve.deployments.llm.vllm.vllm_loggers import (
    RayPrometheusStatLogger,
)
from vllm import AsyncEngineArgs

from vllm.v1.engine.async_llm import AsyncLLM
from vllm.sampling_params import SamplingParams


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


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
