import subprocess
import time
from typing import Literal

import requests
import pytest
from openai import OpenAI
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, ModelLoadingConfig

MODEL_ID = "Qwen/Qwen2.5-0.5B-Instruct"
RAY_MODEL_ID = "qwen-0.5b"
MAX_OUTPUT_TOKENS = 256
SEED = 42


def get_llm_config(
    tensor_parallel_size: int = 1, pipeline_parallel_size: int = 1
) -> LLMConfig:
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
            ),
        ),
        engine_kwargs=dict(
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
        ),
        runtime_env=None,
    )


def start_ray_serve(
    tensor_parallel_size: int = 1, pipeline_parallel_size: int = 1
) -> str:
    """Start Ray Serve with specified parallelism parameters."""
    ray_url = "http://localhost:8000"
    llm_config: LLMConfig = get_llm_config(tensor_parallel_size, pipeline_parallel_size)
    app = build_openai_app({"llm_configs": [llm_config]})
    serve.run(app, blocking=False)
    return ray_url


def create_openai_client(server_url: str) -> OpenAI:
    """Create an OpenAI client."""
    openai_api_base = f"{server_url}/v1"
    return OpenAI(base_url=openai_api_base, api_key="fake-key")


def generate_completion(client: OpenAI, model_id: str, test_prompt: str) -> str:
    """Generate completion using the provided OpenAI client."""
    response = client.completions.create(
        model=model_id,
        prompt=test_prompt,
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
        seed=SEED,
    )
    return response.choices[0].text


def generate_chat_completion(client: OpenAI, model_id: str, test_message: str) -> str:
    """Generate chat completion using the provided OpenAI client."""
    messages = [{"role": "user", "content": test_message}]

    response = client.chat.completions.create(
        model=model_id,
        messages=messages,
        temperature=0.0,
        max_tokens=MAX_OUTPUT_TOKENS,
        seed=SEED,
    )
    return response.choices[0].message.content


class VllmServer:
    def __init__(
        self,
        tensor_parallel_size: int = 1,
        pipeline_parallel_size: int = 1,
        model_id: str = MODEL_ID,
    ):
        self.tensor_parallel_size = tensor_parallel_size
        self.pipeline_parallel_size = pipeline_parallel_size
        self.model_id = model_id
        self.vllm_url = self._start_vllm_server()
        self.openai_client = create_openai_client(self.vllm_url)
        wait_for_server_ready(self.vllm_url, server_type="vllm", timeout=240)

    def _start_vllm_server(self) -> str:
        """Start vLLM server with specified parallelism parameters."""
        vllm_port = 8001
        cmd = [
            "vllm",
            "serve",
            self.model_id,
            "--port",
            str(vllm_port),
            "--distributed-executor-backend=ray",
            "--tensor-parallel-size",
            str(self.tensor_parallel_size),
            "--pipeline-parallel-size",
            str(self.pipeline_parallel_size),
        ]
        self.process = subprocess.Popen(cmd)
        return f"http://localhost:{vllm_port}"

    def generate_completion(self, test_prompt: str) -> str:
        """Generate completion using the provided OpenAI client."""
        return generate_completion(self.openai_client, self.model_id, test_prompt)

    def generate_chat_completion(self, test_message: str) -> str:
        """Generate chat completion using the provided OpenAI client."""
        return generate_chat_completion(self.openai_client, self.model_id, test_message)

    def shutdown(self):
        """Shutdown the vLLM server."""
        self.process.terminate()
        for _ in range(5):
            if self.process.poll() is not None:
                break
            time.sleep(1)
        if self.process.poll() is None:
            self.process.kill()


def wait_for_server_ready(
    url: str,
    server_type: Literal["ray", "vllm"] = "ray",
    timeout: int = 120,
    retry_interval: int = 2,
) -> bool:
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


@pytest.mark.parametrize(
    "tensor_parallel_size, pipeline_parallel_size",
    [
        (1, 1),
        (2, 1),
        (1, 2),
        (2, 2),
    ],
)
def test_llm_serve_correctness(
    tensor_parallel_size: int, pipeline_parallel_size: int
) -> None:
    """Test that Ray Serve and vLLM produce the same completion output for the same input."""
    test_prompt = "Two households, both alike in dignity,"
    test_message = "What is the capital of France?"

    print(
        f"Starting Ray Serve LLM with tensor_parallel_size={tensor_parallel_size}, pipeline_parallel_size={pipeline_parallel_size}"
    )
    ray_url = start_ray_serve(tensor_parallel_size, pipeline_parallel_size)
    ray_client = create_openai_client(ray_url)

    wait_for_server_ready(ray_url, server_type="ray", timeout=240)
    time.sleep(5)  # Buffer time for server to be ready

    ray_completion_output = generate_completion(ray_client, RAY_MODEL_ID, test_prompt)
    ray_chat_output = generate_chat_completion(ray_client, RAY_MODEL_ID, test_message)
    serve.shutdown()

    print(
        f"Starting vLLM server with tensor_parallel_size={tensor_parallel_size}, pipeline_parallel_size={pipeline_parallel_size}"
    )
    vllm_server = VllmServer(tensor_parallel_size, pipeline_parallel_size)
    time.sleep(5)  # Buffer time for server to be ready

    vllm_completion_output = vllm_server.generate_completion(test_prompt)
    vllm_chat_output = vllm_server.generate_chat_completion(test_message)
    vllm_server.shutdown()

    assert ray_completion_output == vllm_completion_output, (
        f"Ray and vLLM outputs do not match with TP={tensor_parallel_size}, PP={pipeline_parallel_size}\n"
        f"Ray output: {ray_completion_output}\n"
        f"vLLM output: {vllm_completion_output}"
    )

    assert ray_chat_output == vllm_chat_output, (
        f"Ray and vLLM chat outputs do not match with TP={tensor_parallel_size}, PP={pipeline_parallel_size}\n"
        f"Ray output: {ray_chat_output}\n"
        f"vLLM output: {vllm_chat_output}"
    )


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
