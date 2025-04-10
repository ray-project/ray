from copy import deepcopy
from typing import List
import sys

import pytest
from fastapi import HTTPException
from ray import serve
from ray.serve.handle import DeploymentHandle

from ray.llm._internal.serve.deployments.llm.llm_server import LLMDeployment
from ray.llm._internal.serve.configs.server_models import LLMConfig, LoraConfig
from ray.llm._internal.serve.deployments.routers.router import (
    LLMRouter,
)
from ray.llm._internal.serve.configs.server_models import ModelData
from ray.llm.tests.serve.deployments.fake_image_retriever import FakeImageRetriever
from ray.llm.tests.serve.deployments.mock_vllm_engine import MockEchoVLLMEngine

VLLM_APP_DEF = """
model_loading_config:
  model_id: meta-llama/Llama-2-7b-hf

llm_engine: vLLM

engine_kwargs:
  trust_remote_code: True
  max_model_len: 4096
  tensor_parallel_size: 1

accelerator_type: A10G

deployment_config:
  autoscaling_config:
    min_replicas: 1
    initial_replicas: 1
    max_replicas: 8
    target_ongoing_requests: 5
    metrics_interval_s: 10.0
    look_back_period_s: 30.0
    smoothing_factor: 1.0
    downscale_delay_s: 300.0
    upscale_delay_s: 60.0
  max_ongoing_requests: 15
  ray_actor_options:
    resources:
      mock_resource: 0

"""


VLLM_APP = LLMConfig.parse_yaml(VLLM_APP_DEF)


# TODO (shrekris): add test for querying fine-tuned weights stored in the
# cloud.


def get_mocked_llm_deployments(llm_configs) -> List[DeploymentHandle]:
    llm_deployments = []
    for llm_config in llm_configs:
        model_id = llm_config.model_id
        deployment_args = llm_config.get_serve_options(name_prefix=f"{model_id}:")
        deployment = LLMDeployment.options(**deployment_args)
        llm_deployments.append(
            deployment.bind(
                llm_config=llm_config,
                engine_cls=MockEchoVLLMEngine,
                image_retriever_cls=FakeImageRetriever,
            )
        )
    return llm_deployments


@pytest.mark.asyncio
async def test_lora_unavailable_base_model(shutdown_ray_and_serve):
    """Getting the handle for an unavailable model should return a 404."""
    llm_config = VLLM_APP.model_copy(deep=True)
    llm_deployments = get_mocked_llm_deployments([llm_config])
    router_deployment = LLMRouter.as_deployment().bind(llm_deployments=llm_deployments)
    router_handle = serve.run(router_deployment)

    with pytest.raises(HTTPException) as e:
        await router_handle._get_configured_serve_handle.remote("anyscale-lora")

    assert e.value.status_code == 404


@pytest.mark.asyncio
async def test_lora_get_model(shutdown_ray_and_serve):
    """Test behavior when getting a LoRA model."""

    base_model_id = "meta-llama/Llama-2-7b-hf"

    llm_config = VLLM_APP.model_copy(deep=True)
    llm_config.model_loading_config.model_id = base_model_id
    llm_deployments = get_mocked_llm_deployments([llm_config])
    router_deployment = LLMRouter.as_deployment().bind(llm_deployments=llm_deployments)
    router_handle = serve.run(router_deployment)

    # Case 1: model does not exist.
    not_found_config = await router_handle.model.remote("not_found")
    assert not_found_config is None

    # Case 2: Model has only the base model config.
    base_model_config = await router_handle.model.remote(base_model_id)
    assert isinstance(base_model_config, ModelData)
    base_model_data = base_model_config.model_dump()
    assert base_model_data["id"] == base_model_id
    base_model_config = base_model_data["rayllm_metadata"]

    # Case 3: model has a multiplex config in the cloud.
    llm_config = VLLM_APP.model_copy(deep=True)
    llm_config.lora_config = LoraConfig(dynamic_lora_loading_path="s3://base_path")
    lora_model = "meta-llama/Llama-2-7b-hf:suffix:1234"
    llm_deployments = get_mocked_llm_deployments([llm_config])

    async def fake_get_lora_model_metadata(*args, **kwargs):
        return {
            "model_id": lora_model,
            "base_model_id": base_model_id,
            "max_request_context_length": 4096,
        }

    router_deployment = LLMRouter.as_deployment().bind(
        llm_deployments=llm_deployments,
        _get_lora_model_metadata_func=fake_get_lora_model_metadata,
    )
    router_handle = serve.run(router_deployment)

    lora_model_config = await router_handle.model.remote(lora_model)
    assert isinstance(lora_model_config, ModelData)
    lora_model_data = lora_model_config.model_dump()
    assert lora_model_data["id"] == lora_model
    lora_metadata = lora_model_data["rayllm_metadata"]
    assert lora_metadata["model_id"] == lora_model
    assert lora_metadata["base_model_id"] == base_model_id
    assert lora_metadata["max_request_context_length"] == 4096


@pytest.mark.asyncio
async def test_lora_list_base_model(shutdown_ray_and_serve):
    """Test model-listing behavior when only the base model is available."""
    base_model_id = "base_model"
    llm_config = VLLM_APP.model_copy(deep=True)
    llm_config.model_loading_config.model_id = base_model_id
    llm_deployments = get_mocked_llm_deployments([llm_config])
    router_deployment = LLMRouter.as_deployment().bind(llm_deployments=llm_deployments)
    router_handle = serve.run(router_deployment)

    models = (await router_handle.models.remote()).data
    assert len(models) == 1

    base_model = models[0]
    base_model_data = base_model.model_dump()
    assert base_model_data["id"] == base_model_id


@pytest.mark.parametrize(
    ("dynamic_lora_loading_path", "base_model_id", "expected_model_ids"),
    [
        # Case 1: test a path that exists in the cloud. The LoRA adapters
        # must be included.
        (
            "s3://anonymous@air-example-data/rayllm-ossci/lora-checkpoints/meta-llama/Llama-2-7b-chat-hf",
            "meta-llama/Llama-2-7b-chat-hf",
            [
                "meta-llama/Llama-2-7b-chat-hf:gen-config-but-no-context-len:1234",
                "meta-llama/Llama-2-7b-chat-hf:with-context-len-and-gen-config:1234",
                "meta-llama/Llama-2-7b-chat-hf:long-context-model:1234",
                "meta-llama/Llama-2-7b-chat-hf",
            ],
        ),
        # Case 2: test a path with the same model provider (meta-llama in this
        # case). But test a different model. Ensure that only this model's
        # LoRA adapters are returned.
        (
            "s3://anonymous@air-example-data/rayllm-ossci/lora-checkpoints/meta-llama/Llama-2-13b-chat-hf",
            "meta-llama/Llama-2-13b-chat-hf",
            [
                "meta-llama/Llama-2-13b-chat-hf:pre-long-context-model:1234",
                "meta-llama/Llama-2-13b-chat-hf",
            ],
        ),
        # Case 3: test a path that doesn't exist in the cloud. Only the
        # base model_id should be included.
        (
            "s3://anonymous@air-example-data/rayllm-ossci/path-does-not-exist/",
            "meta-llama/Llama-2-7b-chat-hf",
            ["meta-llama/Llama-2-7b-chat-hf"],
        ),
    ],
)
@pytest.mark.asyncio
async def test_lora_include_adapters_in_list_models(
    shutdown_ray_and_serve,
    dynamic_lora_loading_path: str,
    base_model_id: str,
    expected_model_ids: List[str],
):
    """Check that LoRA adapters are included in the models list.

    This test pulls real configs from an S3 bucket located in
    `anyscale-legacy-work` account.

    This test is similar to test_lora_list_base_model. It checks that
    the LoRA adapters are included in the list of models.
    """
    app = deepcopy(VLLM_APP)
    app.model_loading_config.model_id = base_model_id
    app.lora_config = LoraConfig(dynamic_lora_loading_path=dynamic_lora_loading_path)

    llm_deployments = get_mocked_llm_deployments([app])
    router_deployment = LLMRouter.as_deployment().bind(llm_deployments=llm_deployments)
    router_handle = serve.run(router_deployment)

    models = (await router_handle.models.remote()).data
    assert {model.id for model in models} == set(expected_model_ids)

    # Confirm that all expected model IDs exist.
    expected_model_ids = set(expected_model_ids)
    for model in models:
        model_data = model.model_dump()
        assert model_data["id"] in expected_model_ids
        expected_model_ids.discard(model_data["id"])

    assert len(expected_model_ids) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
