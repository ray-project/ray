import pytest
import requests
from vllm import LLMClient
from ray.serve.llm import LLMConfig, ModelLoadingConfig
from test_utils import start_service  # adjust import as needed

# Shared LLM configuration
LLM_CONFIG = LLMConfig(
    model_loading_config=ModelLoadingConfig(model_id="llama-1b"),
    runtime_env={"env_vars": {"ENV": "production"}},
    model_loading_config_extra={},  # any additional model loading settings
    accelerator_type="A10G",
    engine_kwargs={"max_model_len": 2048},
    resources_per_bundle={"CPU": 4, "GPU": 1},
)

# Derive deployment and engine configs
SERVE_OPTIONS = LLM_CONFIG.get_serve_options(name_prefix="")
ENGINE_CONFIG = LLM_CONFIG.get_engine_config()
INIT_KWARGS = ENGINE_CONFIG.get_initialization_kwargs()


@pytest.fixture(scope="module")
def ray_service():
    # Start Ray Serve LLM service with identical LLMConfig
    with start_service(
        service_name=SERVE_OPTIONS["name"],
        image_uri=SERVE_OPTIONS.get("user_config", {}).get(
            "image_uri", "your-docker-image:latest"
        ),
        compute_config=LLM_CONFIG.resources_per_bundle,
        applications=["./app_config.yaml"],
        working_dir=".",
        cloud="aws",
        env_vars=LLM_CONFIG.runtime_env.get("env_vars", {}),
        timeout_s=300,
    ) as service_info:
        api_url = service_info["api_url"]
        api_token = service_info["api_token"]
        yield api_url, api_token


@pytest.fixture(scope="module")
def vllm_client():
    # Initialize vLLM client using same engine config
    client = LLMClient(model_name=LLM_CONFIG.model_id, **INIT_KWARGS)
    yield client
    client.close()


def generate_with_ray(api_url: str, api_token: str, prompt: str) -> str:
    headers = {"Authorization": f"Bearer {api_token}"}
    payload = {"prompt": prompt, "max_tokens": 64, "temperature": 0.0}
    resp = requests.post(
        f"{api_url}/{SERVE_OPTIONS['name']}", headers=headers, json=payload
    )
    resp.raise_for_status()
    return resp.json().get("text", "")


def generate_with_vllm(client: LLMClient, prompt: str) -> str:
    result = client.generate(prompt=prompt, max_tokens=64, temperature=0.0)
    return result.completions[0].text if result.completions else ""


@pytest.mark.parametrize(
    "prompt",
    [
        "Translate English to French: 'Hello, how are you?'",
    ],
)
def test_llama1b_ray_vs_vllm(ray_service, vllm_client, prompt):
    """
    Release test: Compare Ray Serve llama-1b output against vLLM reference to ensure fix,
    using identical LLMConfig initialization.
    """
    api_url, api_token = ray_service
    ray_output = generate_with_ray(api_url, api_token, prompt)
    vllm_output = generate_with_vllm(vllm_client, prompt)

    assert ray_output.strip() == vllm_output.strip(), (
        f"Mismatch between Ray Serve and vLLM outputs.\n"
        f"Ray Serve output: {ray_output!r}\n"
        f"vLLM output:      {vllm_output!r}"
    )
