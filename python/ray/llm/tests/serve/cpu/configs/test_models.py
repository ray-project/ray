import sys
from pathlib import Path

import pydantic
import pytest

from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig

CONFIG_DIRS_PATH = str(Path(__file__).parent / "configs")


class TestModelConfig:
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

    def test_invalid_generation_config(self, disable_placement_bundles):
        """Test that passing an invalid generation_config raises an error."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="L4",
                generation_config="invalid_config",  # Should be a dictionary, not a string
            )

    def test_deployment_type_checking(self, disable_placement_bundles):
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

    def test_autoscaling_type_checking(self, disable_placement_bundles):
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

    def test_deployment_unset_fields_are_not_included(self, disable_placement_bundles):
        """Test that unset fields are not included in the deployment config."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
        )
        assert "max_ongoing_requests" not in llm_config.deployment_config
        assert "graceful_shutdown_timeout_s" not in llm_config.deployment_config

    def test_autoscaling_unset_fields_are_not_included(self, disable_placement_bundles):
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

        # Test the core functionality without being strict about Ray's automatic runtime env additions
        assert serve_options["autoscaling_config"] == {
            "min_replicas": 0,
            "initial_replicas": 1,
            "max_replicas": 10,
        }
        assert serve_options["placement_group_bundles"] == [
            {"CPU": 1, "GPU": 0},
            {"GPU": 1, "accelerator_type:A100-40G": 0.001},
        ]
        assert serve_options["placement_group_strategy"] == "STRICT_PACK"
        assert serve_options["name"] == "Test:test_model"

        # Check that our custom env vars are present
        assert (
            serve_options["ray_actor_options"]["runtime_env"]["env_vars"]["FOO"]
            == "bar"
        )
        assert (
            "worker_process_setup_hook"
            in serve_options["ray_actor_options"]["runtime_env"]
        )

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

        # Test the core functionality without being strict about Ray's automatic runtime env additions
        assert serve_options["autoscaling_config"] == {
            "min_replicas": 0,
            "initial_replicas": 1,
            "max_replicas": 10,
        }
        assert serve_options["placement_group_bundles"] == [
            {"CPU": 1, "GPU": 0},
            {"GPU": 1},
        ]
        assert serve_options["placement_group_strategy"] == "STRICT_PACK"
        assert serve_options["name"] == "Test:test_model"

        # Check that our custom env vars are present
        assert (
            serve_options["ray_actor_options"]["runtime_env"]["env_vars"]["FOO"]
            == "bar"
        )
        assert (
            "worker_process_setup_hook"
            in serve_options["ray_actor_options"]["runtime_env"]
        )

    def test_resources_per_bundle(self):
        """Test that resources_per_bundle is correctly parsed."""

        # Test the default resource bundle
        serve_options = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=dict(tensor_parallel_size=3, pipeline_parallel_size=2),
        ).get_serve_options(name_prefix="Test:")
        assert serve_options["placement_group_bundles"] == [{"CPU": 1, "GPU": 0}] + [
            {"GPU": 1} for _ in range(6)
        ]

        # Test the custom resource bundle
        serve_options = LLMConfig(
            model_loading_config=dict(model_id="test_model"),
            engine_kwargs=dict(tensor_parallel_size=3, pipeline_parallel_size=2),
            resources_per_bundle={"XPU": 1},
        ).get_serve_options(name_prefix="Test:")
        assert serve_options["placement_group_bundles"] == [{"CPU": 1, "GPU": 0}] + [
            {"XPU": 1} for _ in range(6)
        ]

    def test_engine_config_cached(self):
        """Test that the engine config is cached and not recreated when calling
        get_engine_config so the attributes on the engine will be persisted."""

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )
        old_engine_config = llm_config.get_engine_config()
        old_engine_config.hf_model_id = "fake_hf_model_id"
        new_engine_config = llm_config.get_engine_config()
        assert new_engine_config is old_engine_config

    def test_experimental_configs(self):
        """Test that `experimental_configs` can be used."""
        # Test with a valid dictionary can be used.
        experimental_configs = {
            "experimental_feature1": "value1",
            "experimental_feature2": "value2",
        }
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs=experimental_configs,
        )
        assert llm_config.experimental_configs == experimental_configs

        # test with invalid dictionary will raise a validation error.
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                ),
                experimental_configs={123: "value1"},
            )

    def test_log_engine_metrics_disable_log_stats_validation(self):
        """Test that log_engine_metrics=True prevents disable_log_stats=True."""
        with pytest.raises(
            pydantic.ValidationError,
            match="disable_log_stats cannot be set to True when log_engine_metrics is enabled",
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                log_engine_metrics=True,
                engine_kwargs={"disable_log_stats": True},
            )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
