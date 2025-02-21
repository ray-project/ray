import pydantic
import pytest
import sys

from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig

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

    def test_deployment_config_extra_forbid(self):
        """Test that deployment config extra is forbid."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                deployment_config={"extra": "invalid"},
            )

    def test_get_serve_options(self):
        """Test that get_serve_options returns the correct options."""
        serve_options = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 0,
                    "initial_replicas": 1,
                    "max_replicas": 10,
                },
            },
            runtime_env={"env_vars": {"FOO": "bar"}},
        ).get_serve_options(name_prefix="Test:")
        expected_options = {
            "autoscaling_config": {
                "min_replicas": 0,
                "initial_replicas": 1,
                "max_replicas": 10,
                "target_num_ongoing_requests_per_replica": 16,
                "target_ongoing_requests": 16,
            },
            "ray_actor_options": {
                "runtime_env": {
                    "env_vars": {"FOO": "bar"},
                    "worker_process_setup_hook": "ray.llm._internal.serve._worker_process_setup_hook",
                }
            },
            "placement_group_bundles": [
                {"CPU": 1, "GPU": 0},
                {"GPU": 1, "accelerator_type:L4": 0.001},
            ],
            "placement_group_strategy": "STRICT_PACK",
            "name": "Test:test_model",
        }
        assert serve_options == expected_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
