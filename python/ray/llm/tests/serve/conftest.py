import ray
from ray import serve
import pytest
from ray.llm._internal.serve.configs.constants import RAYLLM_VLLM_ENGINE_CLS_ENV
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
    DeploymentConfig,
)


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
        "ray.llm._internal.serve.deployments.llm.vllm.vllm_engine.MockVLLMEngine",
    )
    yield


@pytest.fixture
def llm_config(download_model_ckpt):
    yield LLMConfig(
        model_loading_config=ModelLoadingConfig(
            model_id=download_model_ckpt,
        ),
        accelerator_type="L4",
        deployment_config=DeploymentConfig(
            ray_actor_options={"resources": {"mock_resource": 0}},
        ),
    )
