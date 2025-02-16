import pytest
from ray import serve

from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
    ModelLoadingConfig,
    LLMServingArgs,
    DeploymentConfig,
)
from ray.llm._internal.serve.builders.application_builders import (
    build_openai_app,
)
from ray.llm._internal.serve.deployments.llm.vllm.vllm_engine import MockVLLMEngine


@pytest.fixture
def llm_serving_args(download_model_ckpt):
    llm_configs = [
        LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=download_model_ckpt,
            ),
            accelerator_type="L4",
            deployment_config=DeploymentConfig(
                ray_actor_options={"resources": {"mock_resource": 0}},
            ),
        )
    ]
    yield LLMServingArgs(llm_configs=llm_configs)


class TestBuildOpenaiApp:
    def test_build_openai_app_with_config(
        self, llm_serving_args, shutdown_ray_and_serve
    ):
        app = build_openai_app(llm_serving_args, {"engine_cls": MockVLLMEngine})
        assert isinstance(app, serve.Application)
        serve.run(app)


class TestBuildVllmDeployment:
    pass
