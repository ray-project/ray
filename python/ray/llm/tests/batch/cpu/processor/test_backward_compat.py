import sys
import warnings

import pytest

from ray.llm._internal.batch.processor.vllm_engine_proc import vLLMEngineProcessorConfig
from ray.llm._internal.batch.stages.configs import (
    ChatTemplateStageConfig,
    DetokenizeStageConfig,
    PrepareMultimodalStageConfig,
    TokenizerStageConfig,
)


def test_legacy_booleans_coerced_to_stage_configs():
    """Legacy flags → stage configs (dict form)."""
    config = vLLMEngineProcessorConfig(
        model_source="test-model",
        apply_chat_template=True,
        tokenize=False,
        detokenize=True,
    )

    # Legacy flags should be coerced to stage configs
    assert isinstance(config.chat_template_stage, dict)
    assert config.chat_template_stage["enabled"] is True

    assert isinstance(config.tokenize_stage, dict)
    assert config.tokenize_stage["enabled"] is False

    assert isinstance(config.detokenize_stage, dict)
    assert config.detokenize_stage["enabled"] is True


def test_explicit_stage_configs_preserved():
    """Explicit stage configs not overwritten by legacy flags."""
    explicit_chat_template = ChatTemplateStageConfig(enabled=False, batch_size=64)
    config = vLLMEngineProcessorConfig(
        model_source="test-model",
        chat_template_stage=explicit_chat_template,
        apply_chat_template=True,  # Legacy flag should be ignored
    )

    # Explicit stage config should be preserved
    assert config.chat_template_stage is explicit_chat_template
    assert config.chat_template_stage.enabled is False
    assert config.chat_template_stage.batch_size == 64


def test_chat_template_fields_merged():
    """apply_chat_template + chat_template → merged into stage config."""
    config = vLLMEngineProcessorConfig(
        model_source="test-model",
        apply_chat_template=True,
        chat_template="custom_template",
    )

    assert isinstance(config.chat_template_stage, dict)
    assert config.chat_template_stage["enabled"] is True
    assert config.chat_template_stage["chat_template"] == "custom_template"


def test_no_warnings_when_using_new_api():
    """No warnings when only new API used."""
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        vLLMEngineProcessorConfig(
            model_source="test-model",
            chat_template_stage=ChatTemplateStageConfig(enabled=True),
            tokenize_stage=TokenizerStageConfig(enabled=True),
            detokenize_stage=DetokenizeStageConfig(enabled=True),
            prepare_multimodal_stage=PrepareMultimodalStageConfig(enabled=False),
        )
        # Filter out any non-UserWarning warnings
        deprecation_warnings = [
            warning for warning in w if issubclass(warning.category, UserWarning)
        ]
        assert len(deprecation_warnings) == 0


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


@pytest.mark.parametrize("legacy_field", ["has_image", "prepare_image_stage"])
def test_removed_legacy_fields_rejected(legacy_field):
    """Removed legacy fields must raise on construction (pydantic extra=forbid)."""
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        vLLMEngineProcessorConfig(
            model_source="test-model",
            **{legacy_field: True},
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
