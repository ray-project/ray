import contextlib
import pathlib
import tempfile
import time
from typing import Dict
from unittest.mock import patch

import openai
import pytest
import yaml

import ray
from ray import serve
from ray.llm._internal.serve.configs.openai_api_models import (
    ChatCompletionRequest,
    CompletionRequest,
    EmbeddingCompletionRequest,
    ScoreRequest,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import (
    VLLMEngineConfig,
)
from ray.serve.llm import (
    LLMConfig,
    LLMServingArgs,
    ModelLoadingConfig,
    build_openai_app,
)

MOCK_MODEL_ID = "mock-model"


@pytest.fixture
def disable_placement_bundles():
    """
    Fixture to disable placement bundles for tests that don't need GPU hardware.

    Use this fixture in tests that would otherwise require GPU hardware but
    don't actually need to test placement bundle logic.
    """
    with patch.object(
        VLLMEngineConfig,
        "placement_bundles",
        new_callable=lambda: property(lambda self: []),
    ):
        yield


@pytest.fixture
def shutdown_ray_and_serve():
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()
    yield
    serve.shutdown()
    if ray.is_initialized():
        ray.shutdown()


@pytest.fixture
def llm_config(model_pixtral_12b, disable_placement_bundles):
    yield LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_pixtral_12b,
        ),
        accelerator_type="L4",
        runtime_env={},
        log_engine_metrics=False,
    )


@pytest.fixture
def mock_llm_config():
    """LLM config for mock engine testing."""
    return LLMConfig(
        model_loading_config=ModelLoadingConfig(model_id="mock-model"),
        runtime_env={},
        log_engine_metrics=False,
    )


@pytest.fixture
def mock_chat_request(stream, max_tokens):
    """Fixture for creating chat completion requests for mock testing."""
    return ChatCompletionRequest(
        model=MOCK_MODEL_ID,
        messages=[{"role": "user", "content": "Hello, world!"}],
        max_tokens=max_tokens,
        stream=stream,
    )


@pytest.fixture
def mock_completion_request(stream, max_tokens):
    """Fixture for creating text completion requests for mock testing."""
    return CompletionRequest(
        model=MOCK_MODEL_ID,
        prompt="Complete this text:",
        max_tokens=max_tokens,
        stream=stream,
    )


@pytest.fixture
def mock_embedding_request(dimensions):
    """Fixture for creating embedding requests for mock testing."""
    request = EmbeddingCompletionRequest(
        model=MOCK_MODEL_ID,
        input="Text to embed",
    )
    if dimensions:
        request.dimensions = dimensions
    return request


@pytest.fixture
def mock_score_request():
    """Fixture for creating score requests for mock testing."""
    return ScoreRequest(
        model=MOCK_MODEL_ID,
        text_1="What is the capital of France?",
        text_2="The capital of France is Paris.",
    )


def get_test_model_path(yaml_file: str) -> pathlib.Path:
    current_file_dir = pathlib.Path(__file__).absolute().parent
    test_model_path = current_file_dir / yaml_file
    test_model_path = pathlib.Path(test_model_path)

    if not test_model_path.exists():
        raise FileNotFoundError(f"Could not find {test_model_path}")
    return test_model_path


def write_yaml_file(data: Dict) -> pathlib.Path:
    """Writes data to a temporary YAML file and returns the path to it."""

    tmp_file = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False)
    with open(tmp_file.name, "w+") as f:
        yaml.safe_dump(data, f)
    return pathlib.Path(tmp_file.name)


@contextlib.contextmanager
def get_rayllm_testing_model(
    test_model_path: pathlib.Path,
):
    args = LLMServingArgs(llm_configs=[str(test_model_path.absolute())])
    router_app = build_openai_app(args)
    serve._run(router_app, name="router", _blocking=False)

    # Block until the deployment is ready
    # Wait at most 200s [3 min]
    client = openai.Client(
        base_url="http://localhost:8000/v1", api_key="not_an_actual_key"
    )
    model_id = None
    for _i in range(20):
        try:
            models = [model.id for model in client.models.list().data]
            model_id = models[0]
            assert model_id
            break
        except Exception as e:
            print("Error", e)
            pass
        time.sleep(10)
    if not model_id:
        raise RuntimeError("Could not start model!")
    yield client, model_id


@pytest.fixture
def testing_model(shutdown_ray_and_serve, disable_placement_bundles):
    test_model_path = get_test_model_path("mock_vllm_model.yaml")

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id


@pytest.fixture
def testing_model_no_accelerator(shutdown_ray_and_serve, disable_placement_bundles):
    test_model_path = get_test_model_path("mock_vllm_model_no_accelerator.yaml")

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id
