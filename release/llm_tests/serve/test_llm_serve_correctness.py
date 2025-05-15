import os
import pytest
from openai import AsyncOpenAI
from vllm.v1.engine.async_llm import AsyncLLM
from ray.serve.llm import LLMConfig
from probes.query_utils import TextGenerationProbeQuerier
from probes.messages import prompt
from test_utils import start_service, get_current_compute_config_name

# Shared LLM configuration
LLM_CONFIG = LLMConfig(
    model_loading_config=dict(
        model_id="qwen-0.5b",
        model_source="Qwen/Qwen2.5-0.5B-Instruct",
    ),
    accelerator_type="A10G",
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=1,
        )
    ),
    log_engine_metrics=True,
    engine_kwargs=dict(
        tensor_parallel_size=2,
    ),
)


# Derive deployment and engine configs
SERVE_OPTIONS = LLM_CONFIG.get_serve_options(name_prefix="")
ENGINE_CONFIG = LLM_CONFIG.get_engine_config()
INIT_KWARGS = ENGINE_CONFIG.get_initialization_kwargs()
CLOUD = "serve_release_tests_cloud"
SERVICE_NAME = "serve_llm_release_correctness_test_service"


@pytest.fixture(scope="function", autouse=True)
def use_v1_only(monkeypatch):
    """
    The change relies on V1 APIs, so set VLLM_USE_V1=1.
    """
    monkeypatch.setenv("VLLM_USE_V1", "1")


@pytest.fixture(scope="module")
async def openai_async_client():
    """Create an async OpenAI client for testing."""
    async with AsyncOpenAI(
        base_url=SERVE_OPTIONS.get("base_url", "http://localhost:8000"), api_key="test"
    ) as client:
        yield client


@pytest.fixture(scope="module")
def ray_service():
    """Start Ray Serve LLM service with identical LLMConfig."""

    cluster_env = os.environ["ANYSCALE_JOB_CLUSTER_ENV_NAME"]
    image_uri = f"anyscale/image/{cluster_env}:1"
    with start_service(
        service_name=SERVICE_NAME,
        image_uri=image_uri,
        compute_config=get_current_compute_config_name(),
        applications=["./app_config.yaml"],  # TODO: How to set based on LLMConfig?
        working_dir=".",
        cloud=CLOUD,
        env_vars=None,
        timeout_s=300,
    ) as service_info:
        yield service_info


@pytest.fixture(scope="module")
async def vllm_engine():
    """Initialize async vLLM engine using same engine config."""
    engine = AsyncLLM.from_engine_args(**INIT_KWARGS)
    yield engine
    await engine.close()


async def generate_with_ray(
    querier: TextGenerationProbeQuerier, test_prompt: str
) -> str:
    """Generate text using Ray Serve."""
    params = prompt(test_prompt)  # Use prompt format for direct comparison
    response = await querier.query(
        model=LLM_CONFIG.model_loading_config.model_id,
        stream=False,
        chat=False,  # Use completions API for direct comparison
        **params,
    )
    return response.full().strip()


async def generate_with_vllm(engine: AsyncLLM, test_prompt: str) -> str:
    """Generate text using vLLM directly."""
    result = await engine.generate(
        prompt=test_prompt,
        sampling_params=ENGINE_CONFIG.get_sampling_params(
            max_tokens=64, temperature=0.0
        ),
    )
    return result.outputs[0].text.strip() if result.outputs else ""


@pytest.mark.parametrize(
    "test_prompt",
    [
        "Translate English to French: 'Hello, how are you?'",
        "What is 2+2?",
        "Write a haiku about programming.",
    ],
)
@pytest.mark.asyncio
async def test_llama1b_ray_vs_vllm(
    ray_service, openai_async_client, vllm_engine, test_prompt: str, test_id: str
):
    """
    Release test: Compare Ray Serve llama-1b output against vLLM reference to ensure fix,
    using identical LLMConfig initialization.
    """
    querier = TextGenerationProbeQuerier(
        openai_async_client, {"temperature": 0.0, "max_tokens": 64}
    )

    # Add test_id to prompt to ensure uniqueness
    test_prompt = f"{test_id} {test_prompt}"

    ray_output = await generate_with_ray(querier, test_prompt)
    vllm_output = await generate_with_vllm(vllm_engine, test_prompt)

    assert ray_output == vllm_output, (
        f"Mismatch between Ray Serve and vLLM outputs.\n"
        f"Ray Serve output: {ray_output!r}\n"
        f"vLLM output:      {vllm_output!r}"
    )
