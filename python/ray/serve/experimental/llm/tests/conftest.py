import asyncio
import pytest

from ray.serve.experimental.llm.types import SamplingParams


@pytest.fixture
def default_sampling_parameters():
    return SamplingParams(
        temperature=1.0,
        repetition_penalty=1.0,
        top_k=0,
        top_p=1.0,
        typical_p=1.0,
        do_sample=False,
        max_new_tokens=10,
        stop_sequences=[],
        ignore_eos_token=False,
        watermark=False,
        seed=42,
    )


@pytest.fixture(scope="session")
def event_loop():
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
    yield loop
    loop.close()
