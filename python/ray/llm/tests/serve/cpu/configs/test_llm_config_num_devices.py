import sys

import pytest

from ray.serve.llm import LLMConfig


def test_num_devices_vllm_default():
    cfg = LLMConfig(
        model_loading_config=dict(model_source="facebook/opt-125m"),
        llm_engine="vLLM",
        engine_kwargs=dict(tensor_parallel_size=2, pipeline_parallel_size=2),
    )
    assert cfg.num_devices == 4


def test_num_devices_defaults_to_one():
    cfg = LLMConfig(
        model_loading_config=dict(model_source="facebook/opt-125m"),
        llm_engine="vLLM",
    )
    assert cfg.num_devices == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
