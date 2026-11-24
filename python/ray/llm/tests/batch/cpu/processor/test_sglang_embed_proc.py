"""Embed task tests for the SGLang processor. SGLang isn't needed."""

import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ray.data.llm import SGLangEngineProcessorConfig
from ray.llm._internal.batch.constants import SGLangTaskType
from ray.llm._internal.batch.processor import ProcessorBuilder

_EMBED_MODEL = "Qwen/Qwen3-Embedding-0.6B"


@pytest.fixture(autouse=True)
def hf_config():
    """Patch the Hugging Face config the builder loads."""
    config = SimpleNamespace(architectures=["Qwen3ForCausalLM"], vocab_size=1000)
    with patch("transformers.AutoConfig.from_pretrained", return_value=config):
        yield config


def _build(**kwargs):
    return ProcessorBuilder.build(
        SGLangEngineProcessorConfig(
            model_source=_EMBED_MODEL, task_type=SGLangTaskType.EMBED, **kwargs
        )
    )


@pytest.mark.parametrize(
    "kwargs,expected_stages",
    [
        ({}, ["SGLangEngineStage"]),
        ({"chat_template_stage": True}, ["ChatTemplateStage", "SGLangEngineStage"]),
    ],
)
def test_embed_stages(kwargs, expected_stages):
    assert _build(**kwargs).list_stage_names() == expected_stages


def test_embed_engine_kwargs():
    processor = _build(
        engine_kwargs={"is_embedding": False, "skip_tokenizer_init": True}
    )

    engine = processor.get_stage_by_name("SGLangEngineStage")
    engine_kwargs = engine.fn_constructor_kwargs["engine_kwargs"]
    assert engine_kwargs["is_embedding"] is True
    assert engine_kwargs["skip_tokenizer_init"] is False
    assert engine_kwargs["task"] == SGLangTaskType.EMBED


@pytest.mark.parametrize("stage", ["tokenize_stage", "detokenize_stage"])
def test_embed_rejects_unsupported_stage(stage):
    with pytest.raises(ValueError, match=f"doesn't support {stage}"):
        _build(**{stage: True})


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
