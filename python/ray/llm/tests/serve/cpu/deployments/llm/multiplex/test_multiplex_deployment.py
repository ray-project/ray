import sys

import pytest

from ray import serve
from ray.llm._internal.serve.configs.prompt_formats import (
    Prompt,
)
from ray.llm._internal.serve.configs.server_models import (
    LLMConfig,
)
from ray.llm._internal.serve.deployments.llm.llm_server import LLMDeployment
from ray.llm.tests.serve.mocks.mock_vllm_engine import (
    FakeLoraModelLoader,
    MockMultiplexEngine,
)


@pytest.fixture(name="handle")
def handle(shutdown_ray_and_serve):

    llm_config = LLMConfig(
        model_loading_config={
            "model_id": "meta-llama/Llama-2-7b-hf",
        },
        lora_config={
            "max_num_adapters_per_replica": 16,
            "dynamic_lora_loading_path": "s3://my/s3/path_here",
        },
    )

    handle = serve.run(
        LLMDeployment.options(placement_group_bundles=[{"CPU": 1}],).bind(
            llm_config,
            engine_cls=MockMultiplexEngine,
            model_downloader=FakeLoraModelLoader(),
        ),
    )

    return handle


@pytest.mark.asyncio
@pytest.mark.parametrize("stream_tokens", [True, False])
@pytest.mark.parametrize("multiplexed_model_id", ["test_model", None])
async def test_multiplex_deployment(
    handle,
    stream_tokens: bool,
    multiplexed_model_id: str,
):

    gen = handle.options(
        stream=True, multiplexed_model_id=multiplexed_model_id
    )._predict.remote(
        "req_id",
        Prompt(prompt="Generate some sql please.", use_prompt_format=False),
        stream=stream_tokens,
    )

    # gen is an async generator
    # we need to convert it to a list of outputs in one line
    outputs = []
    async for x in gen:
        outputs.append(x)

    assert len(outputs) == 1
    output = outputs[0]

    assert output.stream == stream_tokens

    if multiplexed_model_id is None:
        assert output.disk_multiplex_config is None
    else:
        assert output.disk_multiplex_config.model_dump() == {
            "model_id": multiplexed_model_id,
            "max_total_tokens": None,
            "local_path": "/local/path",
            "lora_assigned_int_id": 1,
        }


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
