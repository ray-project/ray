import pydantic
import pytest

from ray.llm._internal.serve.configs.models import LLMConfig, ModelLoadingConfig

from pathlib import Path

CONFIG_DIRS_PATH = str(Path(__file__).parent / "configs")


class TestModelConfig:
    def test_hf_prompt_format(self):
        """Check that the HF prompt format is correctly parsed."""
        with open(
            f"{CONFIG_DIRS_PATH}/matching_configs/hf_prompt_format.yaml", "r"
        ) as f:
            LLMConfig.parse_yaml(f)

    def test_construction(self):
        """Test construct an LLMConfig doesn't error out and has correct attributes."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            accelerator_type="L4",
        )

        assert llm_config.model_loading_config.model_id == "llm_model_id"
        assert llm_config.accelerator_type == "L4"

    def test_construction_requires_model_loading_config(self):
        """Test that constructing an LLMConfig without model_loading_config errors out"""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                accelerator_type="L4",
            )

    def test_invalid_accelerator_type(self):
        """Test that an invalid accelerator type raises an error."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="INVALID_GPU",  # Should raise error
            )

    def test_invalid_generation_config(self):
        """Test that passing an invalid generation_config raises an error."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="L4",
                generation_config="invalid_config",  # Should be a dictionary, not a string
            )
