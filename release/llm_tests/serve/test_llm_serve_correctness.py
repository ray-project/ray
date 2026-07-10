import subprocess
import time

import pytest
from openai import OpenAI
from ray import serve
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app

from test_utils import create_openai_client, wait_for_server_ready

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
        wait_for_server_ready(self.vllm_url, model_id=self.model_id, timeout=240)

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

    wait_for_server_ready(ray_url, model_id=RAY_MODEL_ID, timeout=240)
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
