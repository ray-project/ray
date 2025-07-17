import pytest

from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig
from ray.llm._internal.serve.deployments.utils.node_initialization_utils import (
    _initialize_local_node,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "model_id,tokenizer_mode,trust_remote_code",
    [
        # Test mistral tokenizer with Tekken-based model
        ("mistralai/Devstral-Small-2505", "mistral", False),
        # Test auto tokenizer with non-Tekken model (Qwen)
        ("Qwen/Qwen2.5-0.5B-Instruct", "auto", False),
        # Test slow tokenizer with non-Tekken model
        ("Qwen/Qwen2.5-0.5B-Instruct", "slow", False),
    ],
)
async def test_tokenizer_download(model_id, tokenizer_mode, trust_remote_code):
    """
    Test that _initialize_local_node correctly downloads and initializes tokenizers
    for different models and tokenizer modes.
    Regression test for https://github.com/ray-project/ray/issues/53873
    """
    llm_config = LLMConfig(
        runtime_env={},
        model_loading_config=ModelLoadingConfig(
            model_id=model_id,
        ),
        log_engine_metrics=False,
        engine_kwargs={
            "tokenizer_mode": tokenizer_mode,
            "enforce_eager": True,
            "trust_remote_code": trust_remote_code,
        },
    )

    await _initialize_local_node(
        llm_config,
        download_model=NodeModelDownloadable.TOKENIZER_ONLY,
        download_extra_files=False,
    )


if __name__ == "__main__":
    pytest.main([__file__])
