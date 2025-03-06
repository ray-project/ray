import pytest
import ray
import ray.serve.context
from ray import serve
import sys

from ray.llm._internal.serve.deployments.llm.llm_server import LLMDeployment
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.configs.prompt_formats import (
    Prompt,
)
from ray.llm.tests.serve.deployments.mock_vllm_engine import (
    FakeLoraModelLoader,
    MockMultiplexEngine,
)

vllm_app_def = """
model_loading_config:
  model_id: meta-llama/Llama-2-7b-hf

llm_engine: vLLM

engine_kwargs:
  trust_remote_code: True
  max_model_len: 4096

accelerator_type: A10G

lora_config:
  max_num_adapters_per_replica: 16
  dynamic_lora_loading_path: s3://my/s3/path_here

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
      accelerator_type_a10: 0

"""  # noqa

test_lora_multiplex_config = """
base_model_id: meta-llama/Llama-2-7b-hf
model_id: test_model
max_total_tokens: 4096
lora_mirror_config:
  bucket_uri: s3://endpoints-finetune-mirrors/dev-test/tchordia/sql-lora-adapter/
"""

expected_lora_out = {
    "prompt": "Generate some sql please.",
    "request_id": "req_id",
    "sampling_params": {
        "max_tokens": None,
        "temperature": None,
        "top_p": None,
        "n": 1,
        "logprobs": None,
        "top_logprobs": None,
        "logit_bias": None,
        "stop": [],
        "stop_tokens": [],
        "ignore_eos": None,
        "presence_penalty": None,
        "frequency_penalty": None,
        "best_of": 1,
        "response_format": None,
        "top_k": None,
        "seed": None,
    },
    "multi_modal_data": None,
    "disk_multiplex_config": {
        "model_id": "test_model",
        "base_model_id": "meta-llama/Llama-2-7b-hf",
        "model_type": "text-generation-finetuned",
        "max_total_tokens": 4096,
        "lora_mirror_config": {
            "bucket_uri": "s3://endpoints-finetune-mirrors/dev-test/tchordia/sql-lora-adapter/"
        },
        "local_path": "/local/path",
        "lora_assigned_int_id": 1,
        "enabled": True,
    },
}


@pytest.fixture
def initialize_ray():
    if not ray.is_initialized():
        ray.init()

    yield

    if ray.is_initialized():
        serve.shutdown()
        ray.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_tokens", [True, False])
async def test_multiplex_deployment(
    initialize_ray,
    stream_tokens: bool,
):
    vllm_app = LLMConfig.parse_yaml(vllm_app_def)

    handle = serve._run(
        LLMDeployment.options(placement_group_bundles=[{"CPU": 1}],).bind(
            vllm_app,
            engine_cls=MockMultiplexEngine,
            model_downloader=FakeLoraModelLoader(),
        ),
        route_prefix="/vllm_app",
    )
    print(handle)
    responses = [
        x
        async for x in handle.options(
            stream=True, multiplexed_model_id="test_model"
        )._predict.remote(
            "req_id",
            Prompt(prompt="Generate some sql please.", use_prompt_format=False),
            stream=stream_tokens,
        )
    ]

    # This should be the arg we pass into the engine generate
    arg = responses[0]

    # All inputs should match
    assert responses[1:] == list(range(10))

    assert arg is not None

    expected_lora_out_with_serve_request_context = dict(expected_lora_out)
    expected_lora_out_with_serve_request_context[
        "serve_request_context"
    ] = arg.model_dump().get("serve_request_context")
    print("***arg***", arg.model_dump())
    print("***exp***", expected_lora_out_with_serve_request_context)
    assert arg == arg.__class__(**expected_lora_out_with_serve_request_context)

    responses = [
        x
        async for x in handle.options(stream=True)._predict.remote(
            "req_id",
            Prompt(prompt="Generate some sql please.", use_prompt_format=False),
            stream=stream_tokens,
        )
    ]
    arg = responses[0]

    # All inputs should match
    assert responses[1:] == list(range(10))

    assert arg is not None
    print("**baseout**", arg.model_dump())
    expected_model_dump = {
        "prompt": "Generate some sql please.",
        "request_id": "req_id",
        "sampling_params": {
            "max_tokens": None,
            "temperature": None,
            "top_p": None,
            "stop": [],
            "stop_tokens": [],
            "ignore_eos": None,
            "presence_penalty": None,
            "frequency_penalty": None,
            "top_k": None,
            "response_format": None,
            "logprobs": None,
            "top_logprobs": None,
            "seed": None,
            "logit_bias": None,
            "n": 1,
            "best_of": 1,
        },
        "multi_modal_data": None,
        "serve_request_context": arg.model_dump().get("serve_request_context"),
        "disk_multiplex_config": None,
    }
    assert arg.model_dump() == expected_model_dump, (
        "Arg model dump didn't match expected value."
        f"\n\nModel dump: {arg.model_dump()}"
        f"\n\nExpected model dump: {expected_model_dump}"
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
