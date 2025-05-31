import contextlib
import pathlib
import tempfile
import time
from typing import Dict

import openai
import pytest
import yaml

import ray
from ray import serve
from ray.llm._internal.serve.builders.application_builders import build_openai_app
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    LLMServingArgs,
    ModelLoadingConfig,
)
from ray.serve.llm import LLMServer


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
def testing_model(shutdown_ray_and_serve):
    test_model_path = get_test_model_path("mock_vllm_model.yaml")

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id


@pytest.fixture
def testing_model_no_accelerator(shutdown_ray_and_serve):
    test_model_path = get_test_model_path("mock_vllm_model_no_accelerator.yaml")

    with get_rayllm_testing_model(test_model_path) as (client, model_id):
        yield client, model_id


@pytest.fixture
def create_server():
    """Asynchronously create an LLMServer instance."""

    async def creator(*args, **kwargs):
        # _ = LLMServer(...) will raise TypeError("__init__() should return None")
        # so we do __new__ then __init__
        server = LLMServer.__new__(LLMServer)
        await server.__init__(*args, **kwargs)
        return server

    return creator
