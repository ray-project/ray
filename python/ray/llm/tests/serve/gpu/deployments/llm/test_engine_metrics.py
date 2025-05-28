import pytest
import sys
import os

from ray.llm._internal.serve.deployments.llm.vllm.vllm_loggers import (
    RayPrometheusStatLogger,
)
from ray.llm._internal.utils import try_import
from vllm import AsyncEngineArgs

from vllm.v1.engine.async_llm import AsyncLLM
from vllm.sampling_params import SamplingParams


vllm = try_import("vllm")


@pytest.mark.asyncio
async def test_engine_metrics():
    """
    Test that the stat logger can be created successfully.
    """

    # Set VLLM_USE_V1 to 1 to use V1 APIs
    os.environ["VLLM_USE_V1"] = "1"
    engine_args = AsyncEngineArgs(
        model="unsloth/Llama-3.2-1B-Instruct",
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
