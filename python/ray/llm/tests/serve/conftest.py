import ray
from ray import serve
import pytest
from ray.llm._internal.serve.configs.constants import RAYLLM_VLLM_ENGINE_CLS_ENV
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMServingArgs,
)
import yaml
from typing import Dict
import pathlib
import tempfile
import contextlib
from ray.llm._internal.serve.builders.application_builders import build_openai_app
import openai
import time


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
def use_mock_vllm_engine(monkeypatch):
    monkeypatch.setenv(
        RAYLLM_VLLM_ENGINE_CLS_ENV,
        "ray.llm.tests.serve.deployments.mock_vllm_engine.MockVLLMEngine",
    )
    yield


@pytest.fixture
def llm_config(model_pixtral_12b):
    yield LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=model_pixtral_12b,
        ),
        accelerator_type="L4",
        deployment_config=dict(
            ray_actor_options={"resources": {"mock_resource": 0}},
        ),
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
def testing_model(shutdown_ray_and_serve, use_mock_vllm_engine, model_pixtral_12b):
    test_model_path = get_test_model_path("mock_vllm_model.yaml")

    with open(test_model_path, "r") as f:
        loaded_llm_config = yaml.safe_load(f)

    loaded_llm_config["model_loading_config"]["model_source"] = model_pixtral_12b
    test_model_path = write_yaml_file(loaded_llm_config)

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id


@pytest.fixture
def testing_model_no_accelerator(
    shutdown_ray_and_serve, use_mock_vllm_engine, model_pixtral_12b
):
    test_model_path = get_test_model_path("mock_vllm_model_no_accelerator.yaml")

    with open(test_model_path, "r") as f:
        loaded_llm_config = yaml.safe_load(f)

    loaded_llm_config["model_loading_config"]["model_source"] = model_pixtral_12b
    test_model_path = write_yaml_file(loaded_llm_config)

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id
