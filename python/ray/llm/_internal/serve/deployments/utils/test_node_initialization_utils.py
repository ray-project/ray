import pytest

from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
from ray.llm._internal.serve.deployments.llm.vllm.vllm_models import VLLMEngineConfig
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    _initialize_local_node,
)


@pytest.mark.asyncio
async def test_mistral_tokenizer_download():
    """
    Test that _initialize_local_node correctly downloads and initializes mistral tokenizer.
    Regression test for https://github.com/ray-project/ray/issues/53873
    """
    engine_config = VLLMEngineConfig(
        model_id="mistralai/Devstral-Small-2505",
        hf_model_id="mistralai/Devstral-Small-2505",
        mirror_config=None,
        accelerator_type=None,
        engine_kwargs={
            "tokenizer_mode": "mistral",
            "enforce_eager": True,
            "trust_remote_code": False,
        },
    )

    await _initialize_local_node(
        engine_config,
        download_model=NodeModelDownloadable.TOKENIZER_ONLY,
        download_extra_files=False,
    )
