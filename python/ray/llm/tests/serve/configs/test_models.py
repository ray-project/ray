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
            accelerator_type="A100-40G",  # Dash instead of underscore when specifying accelerator type
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 3,
                    "max_replicas": 7,
                }
            },
        )
        assert llm_config.deployment_config["autoscaling_config"]["min_replicas"] == 3
        assert llm_config.deployment_config["autoscaling_config"]["max_replicas"] == 7
        assert llm_config.model_loading_config.model_id == "llm_model_id"
        assert llm_config.accelerator_type == "A100-40G"

    def test_construction_requires_model_loading_config(self):
        """Test that constructing an LLMConfig without model_loading_config errors out"""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                accelerator_type="L4",
            )

    def test_accelerator_type_optional(self):
        """Test that accelerator_type is optional when initializing LLMConfig."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model")
        )
        assert llm_config.model_loading_config.model_id == "test_model"
        assert llm_config.accelerator_type is None

    def test_invalid_accelerator_type(self):
        """Test that invalid accelerator types raise validation errors."""
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="INVALID_GPU",  # Invalid string value
            )

        # Test invalid numeric value
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type=123,  # Must be a string
            )

        # Test that underscore is not supported in accelerator type
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="A100_40G",  # Should use A100-40G instead
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

    def test_deployment_type_checking(self):
        """Test that deployment config type checking works."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                deployment_config={
                    "max_ongoing_requests": -1,
                },
                accelerator_type="L4",
            )

    def test_autoscaling_type_checking(self):
        """Test that autoscaling config type checking works."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                deployment_config={
                    "autoscaling_config": {
                        "min_replicas": -1,
                    },
                },
                accelerator_type="L4",
            )

    def test_deployment_unset_fields_are_not_included(self):
        """Test that unset fields are not included in the deployment config."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
        )
        assert "max_ongoing_requests" not in llm_config.deployment_config
        assert "graceful_shutdown_timeout_s" not in llm_config.deployment_config

    def test_autoscaling_unset_fields_are_not_included(self):
        """Test that unset fields are not included in the autoscaling config."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 3,
                    "max_replicas": 7,
                },
            },
            accelerator_type="L4",
        )
        assert (
            "metrics_interval_s"
            not in llm_config.deployment_config["autoscaling_config"]
        )
        assert (
            "upscaling_factor" not in llm_config.deployment_config["autoscaling_config"]
        )

    def test_get_serve_options_with_accelerator_type(self):
        """Test that get_serve_options returns the correct options when accelerator_type is set."""
        serve_options = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="A100-40G",
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
            },
            "ray_actor_options": {
                "runtime_env": {
                    "env_vars": {"FOO": "bar"},
                    "worker_process_setup_hook": "ray.llm._internal.serve._worker_process_setup_hook",
                }
            },
            "placement_group_bundles": [
                {"CPU": 1, "GPU": 0},
                {"GPU": 1, "accelerator_type:A100-40G": 0.001},
            ],
            "placement_group_strategy": "STRICT_PACK",
            "name": "Test:test_model",
        }
        assert serve_options == expected_options

    def test_get_serve_options_without_accelerator_type(self):
        """Test that get_serve_options returns the correct options when accelerator_type is not set."""
        serve_options = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
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
            },
            "ray_actor_options": {
                "runtime_env": {
                    "env_vars": {"FOO": "bar"},
                    "worker_process_setup_hook": "ray.llm._internal.serve._worker_process_setup_hook",
                }
            },
            "placement_group_bundles": [
                {"CPU": 1, "GPU": 0},
                {"GPU": 1},
            ],
            "placement_group_strategy": "STRICT_PACK",
            "name": "Test:test_model",
        }
        assert serve_options == expected_options


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
