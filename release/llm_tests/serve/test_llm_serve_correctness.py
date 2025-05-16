import subprocess
import time
import requests
from openai import OpenAI
from ray import serve

from ray.serve.llm import LLMConfig, build_openai_app

MODEL_ID = "Qwen/Qwen2.5-1.5B-Instruct"
RAY_MODEL_ID = "qwen-1.5b"

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id=RAY_MODEL_ID,
        model_source=MODEL_ID,
    ),
    deployment_config=dict(
        autoscaling_config=dict(
            min_replicas=1, max_replicas=1,
        )
    ),
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

    response = client.completions.create(model=RAY_MODEL_ID, prompt=test_prompt, temperature=0.0)
    return response.choices[0].text


def generate_with_vllm(test_prompt, vllm_server_url):
    openai_api_key = "EMPTY"
    openai_api_base = f"{vllm_server_url}/v1"
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    response = client.completions.create(model=MODEL_ID, prompt=test_prompt, temperature=0.0)
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
    vllm_url, vllm_process = start_vllm_server()

    wait_for_server_ready(ray_url)
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

# Next step: Need to increase max tokens for vLLM
# Ray and vLLM outputs do not match
# vLLM output:  I'm a beginner in Python and I'm trying to understand how to use the
# Ray output:  I'm a beginner in Python and I'm trying to understand how to use the `map()` function. I have a list of numbers and I want to apply a function to each element in the list. I've seen examples where `map()` is used with a lambda function, but I'm not sure how to use it with a regular function. Can someone provide an example of how to use `map()` with a regular function in Python? Additionally, I'm curious about the difference between using `map()` with a lambda function and using a regular function. Could you explain that as well? Sure, I can help with that! Let's start by looking at an example of using `map()` with a lambda function. Here's a simple example:

# ```python
# # Define a list of numbers
# numbers = [1, 2, 3, 4, 5]

# # Use map() with a lambda function to square each number
# squared_numbers = map(lambda x: x ** 2, numbers)

# # Convert the map object to a list to see the results
# squared_numbers_list = list(squared_numbers)
# print(squared_numbers_list)  # Output: [1, 4, 9, 16, 25]
# ```

# In this example, we're using a lambda function to square each number in the list. The `map()` function applies this lambda function to each element in the `numbers` list, and the result is a map object. We then convert this map object to a list to see the results.

# Now, let's look at an example of using `map()` with a regular function. Here's an example where we're using a regular function to calculate the square root of each number in the list:

# ```python
# # Define a list of numbers
# numbers = [1, 4, 9, 16, 25]

# # Use map() with a regular function to calculate the square root of each number
# square_roots = map(lambda x: x ** 0.5, numbers)

# # Convert the map object to a list to see the results
# square_roots_list = list(square_roots)
# print(square_roots_list)  # Output: [1.0, 2.0, 3.0, 4.0, 5.0]
# ```

# In this example, we're using a lambda function to calculate the square root of each number in the list. The `map()` function applies this lambda function to each element in the `numbers` list, and the result is a map object. We then convert this map object to a list to see the results.

# The main difference between using `map()` with a lambda function and using a regular function is that the lambda function is a one-line function that takes a single argument and returns the result of the operation. The regular function, on the other hand, is a full function that takes a single argument and returns the result of the operation. In this case, both examples use the same lambda function to perform the same operation on each element in the list. However, if you have a more complex operation that you want to perform on each element in the list, you would need to define a regular function instead of using a lambda function.
