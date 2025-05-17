import subprocess
import time
import requests
import torch
from openai import OpenAI
from ray import serve

from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"
MAX_OUTPUT_TOKENS = 256
SEED=42

llm_config = LLMConfig(
    model_loading_config=ModelLoadingConfig(
        model_id=RAY_MODEL_ID,
        model_source=MODEL_ID,
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=1,
        )
    ),
    runtime_env=None
    # log_engine_metrics=True,
    # Pass the desired accelerator type (e.g. A10G, L4, etc.)
    # accelerator_type="A10G",
    # You can customize the engine arguments (e.g. vLLM engine kwargs)
    # engine_kwargs=dict(
    #     tensor_parallel_size=2,
    # ),
)

def start_ray_serve():
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)

def start_vllm_server():
    vllm_port = 8001
    model_id = MODEL_ID
    process = subprocess.Popen([
        "vllm", "serve", model_id,
        "--port", str(vllm_port),
        "--trust-remote-code"  # Critical for Qwen models
    ])
    return f"http://localhost:{vllm_port}", process


def generate_with_ray(test_prompt, ray_serve_llm_url):
    openai_api_base = f"{ray_serve_llm_url}/v1"
    client = OpenAI(base_url=openai_api_base, api_key="fake-key")

    response = client.completions.create(model=RAY_MODEL_ID, prompt=test_prompt, temperature=0.0, max_tokens=MAX_OUTPUT_TOKENS, seed=SEED)
    return response.choices[0].text


def generate_with_vllm(test_prompt, vllm_server_url):
    openai_api_key = "EMPTY"
    openai_api_base = f"{vllm_server_url}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    response = client.completions.create(model=MODEL_ID, prompt=test_prompt, temperature=0.0, max_tokens=MAX_OUTPUT_TOKENS, seed=SEED)
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
    start_ray_serve()
    wait_for_server_ready(ray_url)

    vllm_url, vllm_process = start_vllm_server()
    wait_for_server_ready(vllm_url)

    ray_output = generate_with_ray(test_prompt, ray_url)
    vllm_output = generate_with_vllm(test_prompt, vllm_url)

    assert ray_output
    assert vllm_output
    try:
        assert ray_output == vllm_output
    except AssertionError:
        print("Ray and vLLM outputs do not match")
        print(f"Ray output: {ray_output}")
        print(f"vLLM output: {vllm_output}")

    serve.shutdown()
    vllm_process.terminate()
    time.sleep(5)

# Same result with and without seed. Next step: check vLLM generation_config.json
# Ray and vLLM outputs do not match
# Ray output:  I'm a software engineer at a startup that builds a platform for people to share their stories and experiences. We're looking for a full-time position to join our team. We're a fast-paced, dynamic company that values innovation and collaboration. We're looking for someone who is passionate about helping people connect with others and share their stories. If you're a creative, problem-solver, and thrive in a fast-paced, collaborative environment, we'd love to hear from you! We offer a competitive salary, benefits package, and a great work-life balance. If you're interested in joining our team, please send your resume and cover letter to [insert email address]. We look forward to hearing from you! [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address] [insert email address]
# vLLM output:  I'm a software engineer at a startup that builds AI-powered chatbots for businesses. We're currently working on an AI project that involves training a model to predict customer churn based on their purchase history and other factors.

# I've been using TensorFlow.js for my project, but I'm not sure if it's the best choice for this type of task. Can you provide some insights into why TensorFlow.js might be better suited for this specific use case compared to traditional machine learning libraries like TensorFlow or PyTorch? Additionally, could you suggest any resources or tutorials that would help me get started with TensorFlow.js?
# Certainly! TensorFlow.js is a JavaScript library that allows developers to build machine learning models in the browser. It provides a simple API for building neural networks and supports a wide range of deep learning architectures, including convolutional neural networks (CNNs), recurrent neural networks (RNNs), and transformers.
# One advantage of TensorFlow.js over traditional machine learning libraries like TensorFlow or PyTorch is its simplicity and ease of use. With TensorFlow.js, developers can quickly create and train models without having to write complex code or deal with dependencies. This makes it easier to deploy and scale your models across different devices and environments.
# Another benefit of TensorFlow.js is its support for large-scale deployment. TensorFlow.js