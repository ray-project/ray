import os
import subprocess
import time
import requests
from openai import OpenAI
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"
MAX_OUTPUT_TOKENS = 256
SEED = 42

# vLLM has started using this with V1 instead of xgrammar
os.environ["RAYLLM_GUIDED_DECODING_BACKEND"] = "auto"

llm_config = LLMConfig(
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
    runtime_env=None,
)


def start_ray_serve():
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)


def start_vllm_server():
    vllm_port = 8001
    model_id = MODEL_ID
    process = subprocess.Popen(
        [
            "vllm",
            "serve",
            model_id,
            "--port",
            str(vllm_port),
            "--distributed-executor-backend=ray",
            "--generation-config=vllm",
        ]
    )
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


def wait_for_server_ready(url, timeout=120, retry_interval=2):
    """Poll the server until it's ready or timeout is reached."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{url}/v1/models", timeout=5)
            if response.status_code == 200:
                print(f"Server at {url} is ready!")
                return True
        except (requests.RequestException, ConnectionError):
            pass
        time.sleep(retry_interval)

    raise TimeoutError(f"Server at {url} did not become ready within {timeout} seconds")


if __name__ == "__main__":
    test_prompt = "Hello, world!"
    test_id = "test_1"
    ray_url = "http://localhost:8000"

    try:
        start_ray_serve()
        wait_for_server_ready(ray_url)

        # Use global vllm_process but don't re-declare it inside the function
        vllm_url, vllm_process = start_vllm_server()

        wait_for_server_ready(vllm_url)

        ray_output = generate_with_ray(test_prompt, ray_url)
        vllm_output = generate_with_vllm(test_prompt, vllm_url)

        try:
            assert ray_output == vllm_output
        except AssertionError:
            print("Ray and vLLM outputs do not match")
            print(f"Ray output: {ray_output}")
            print(f"vLLM output: {vllm_output}")
    finally:
        serve.shutdown()
        vllm_process.terminate()

        for _ in range(5):
            if vllm_process.poll() is not None:
                break
            time.sleep(1)
        if vllm_process.poll() is None:
            vllm_process.kill()
