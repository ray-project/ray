import os
import subprocess
import time
import requests
import pytest
from openai import OpenAI
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"
MAX_OUTPUT_TOKENS = 256
SEED = 42

# vLLM has started using this with V1 instead of xgrammar
os.environ["RAYLLM_GUIDED_DECODING_BACKEND"] = "auto"


def get_llm_config(tensor_parallel_size=1, pipeline_parallel_size=1):
    """Create LLMConfig with specified parallelism parameters."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=RAY_MODEL_ID,
            model_source=MODEL_ID,
        ),
        deployment_config=dict(
            autoscaling_config=dict(
                min_replicas=1,
                max_replicas=1,
            )
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
        ),
        runtime_env=None,
    )


def start_ray_serve(tensor_parallel_size=1, pipeline_parallel_size=1):
    """Start Ray Serve with specified parallelism parameters."""
    ray_url = "http://localhost:8000"
    llm_config = get_llm_config(tensor_parallel_size, pipeline_parallel_size)
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    return ray_url


def start_vllm_server(tensor_parallel_size=1, pipeline_parallel_size=1):
    """Start vLLM server with specified parallelism parameters."""
    vllm_port = 8001
    cmd = [
        "vllm",
        "serve",
        MODEL_ID,
        "--port",
        str(vllm_port),
        "--distributed-executor-backend=ray",
        "--generation-config=vllm",  # Force vLLM to ignore HF generation_config.json
        "--tensor-parallel-size",
        str(tensor_parallel_size),
        "--pipeline-parallel-size",
        str(pipeline_parallel_size),
    ]
    process = subprocess.Popen(cmd)
    return f"http://localhost:{vllm_port}", process


def generate_with_ray(test_prompt, ray_serve_llm_url):
    openai_api_base = f"{ray_serve_llm_url}/v1"
    client = OpenAI(base_url=openai_api_base, api_key="fake-key")

    response = client.completions.create(
        model=RAY_MODEL_ID,
        prompt=test_prompt,
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
        seed=SEED,
    )
    return response.choices[0].text


def generate_with_vllm(test_prompt, vllm_server_url):
    openai_api_key = "EMPTY"
    openai_api_base = f"{vllm_server_url}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    response = client.completions.create(
        model=MODEL_ID,
        prompt=test_prompt,
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
        seed=SEED,
    )
    return response.choices[0].text


def wait_for_server_ready(url, server_type="ray", timeout=120, retry_interval=2):
    """Poll the server until it's ready or timeout is reached.

    Args:
        url: The server URL to check
        server_type: Either "ray" or "vllm"
        timeout: Maximum time to wait in seconds
        retry_interval: Time between retry attempts
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            # Directly test if the server can handle a completion request
            model_id = MODEL_ID if server_type == "vllm" else RAY_MODEL_ID
            test_data = {
                "model": model_id,
                "prompt": "test",
                "max_tokens": 5,
                "temperature": 0,
            }
            completion_response = requests.post(
                f"{url}/v1/completions", json=test_data, timeout=10
            )
            if completion_response.status_code == 200:
                print(
                    f"{server_type.upper()} server at {url} is ready to handle requests!"
                )
                return True
        except Exception:
            pass

        print(f"Waiting for {server_type.upper()} server at {url} to be ready...")
        time.sleep(retry_interval)

    raise TimeoutError(
        f"{server_type.upper()} server at {url} did not become ready within {timeout} seconds"
    )


@pytest.fixture(
    params=[
        # (tensor_parallel_size, pipeline_parallel_size)
        (1, 1),
        (2, 1),
        (1, 2),
        (2, 2),
    ],
    ids=[
        "tp1-pp1",
        "tp2-pp1",
        "tp1-pp2",
        "tp2-pp2",
    ],
)
def setup_servers(request):
    """Fixture to set up Ray Serve and vLLM servers with different parallelism settings."""
    tensor_parallel_size, pipeline_parallel_size = request.param

    print(
        f"Setting up servers with TP={tensor_parallel_size}, PP={pipeline_parallel_size}"
    )

    # Start Ray Serve
    ray_url = start_ray_serve(tensor_parallel_size, pipeline_parallel_size)

    # Start vLLM Server
    vllm_url, vllm_process = start_vllm_server(
        tensor_parallel_size, pipeline_parallel_size
    )

    # Wait for servers to be ready
    wait_for_server_ready(vllm_url, server_type="vllm", timeout=240)
    wait_for_server_ready(ray_url, server_type="ray", timeout=240)
    time.sleep(5)  # Buffer time for the servers to be ready

    # Provide the server URLs to the test
    yield ray_url, vllm_url, vllm_process, tensor_parallel_size, pipeline_parallel_size

    # Cleanup
    serve.shutdown()
    vllm_process.terminate()

    for _ in range(5):
        if vllm_process.poll() is not None:
            break
        time.sleep(1)
    if vllm_process.poll() is None:
        vllm_process.kill()


def test_llm_serve_correctness(setup_servers):
    """Test that Ray Serve and vLLM produce the same output for the same input."""
    ray_url, vllm_url, _, tp_size, pp_size = setup_servers
    test_prompt = "Hello, world!"

    print(
        f"Testing with tensor_parallel_size={tp_size}, pipeline_parallel_size={pp_size}"
    )

    ray_output = generate_with_ray(test_prompt, ray_url)
    vllm_output = generate_with_vllm(test_prompt, vllm_url)

    assert ray_output == vllm_output, (
        f"Ray and vLLM outputs do not match with TP={tp_size}, PP={pp_size}\n"
        f"Ray output: {ray_output}\n"
        f"vLLM output: {vllm_output}"
    )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
