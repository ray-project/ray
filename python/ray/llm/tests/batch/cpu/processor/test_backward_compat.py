import sys

import pytest

from ray.llm._internal.batch.processor.vllm_engine_proc import vLLMEngineProcessorConfig


def test_legacy_dict_stage_config():
    """Dict form stage configs work correctly."""
    config = vLLMEngineProcessorConfig(
        model_source="test-model",
        chat_template_stage={"enabled": False, "batch_size": 128},
        tokenize_stage={"enabled": True, "concurrency": 4},
    )

    assert isinstance(config.chat_template_stage, dict)
    assert config.chat_template_stage["enabled"] is False
    assert config.chat_template_stage["batch_size"] == 128

    assert isinstance(config.tokenize_stage, dict)
    assert config.tokenize_stage["enabled"] is True
    assert config.tokenize_stage["concurrency"] == 4


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
