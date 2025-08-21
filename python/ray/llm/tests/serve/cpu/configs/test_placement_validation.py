import sys

import pytest

from ray.llm._internal.serve.configs.server_models import LLMConfig, ModelLoadingConfig


class TestPlacementGroupValidation:
    """Test placement group validation for custom placement group configurations."""

    def test_missing_placement_strategy(self, disable_placement_bundles):
        """Test that missing placement_group_strategy raises appropriate error."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={"placement_group_bundles": [{"GPU": 1}]},
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError,
            match="placement_group_bundles specified without placement_group_strategy",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_missing_placement_bundles(self, disable_placement_bundles):
        """Test that missing placement_group_bundles raises appropriate error."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={"placement_group_strategy": "PACK"},
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError,
            match="placement_group_strategy specified without placement_group_bundles",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_placement_strategy(self, disable_placement_bundles):
        """Test that invalid placement strategy is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}],
                "placement_group_strategy": "INVALID_STRATEGY",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="Invalid placement_group_strategy: INVALID_STRATEGY"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_empty_placement_bundles(self, disable_placement_bundles):
        """Test that empty placement_group_bundles list is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="placement_group_bundles must be a non-empty list"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_non_dict_bundle_entry(self, disable_placement_bundles):
        """Test that non-dictionary bundle entries are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": ["not_a_dict"],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="Bundle 0 must be a dictionary"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_negative_bundle_resources(self, disable_placement_bundles):
        """Test that negative resource amounts in bundles are rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            deployment_config={
                "placement_group_bundles": [{"GPU": -1}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="Bundle 0 resource 'GPU' must be a non-negative number"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_insufficient_gpu_resources_for_parallelism(
        self, disable_placement_bundles
    ):
        """Test that insufficient GPU resources for TP/PP configuration are caught."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 4,
                "pipeline_parallel_size": 2,
            },
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}],  # Only 2 GPUs, need 8
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="Insufficient GPU resources"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_rank_mapping_length(self, disable_placement_bundles):
        """Test that rank_to_bundle_index with incorrect length is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 2,
            },
            rank_to_bundle_index=[0, 1],  # Wrong length, need 4 entries
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(ValueError, match="rank_to_bundle_index must have length 4"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_rank_mapping_index(self, disable_placement_bundles):
        """Test that rank_to_bundle_index with invalid bundle indices is rejected."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 1,
            },
            rank_to_bundle_index=[0, 5],  # Bundle index 5 doesn't exist
            deployment_config={
                "placement_group_bundles": [{"GPU": 1}, {"GPU": 1}],  # Only indices 0,1
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        with pytest.raises(
            ValueError, match="rank_to_bundle_index\\[1\\] = 5 is invalid"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_valid_custom_placement_configuration(self, disable_placement_bundles):
        """Test that valid custom placement group configuration passes validation."""
        config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test-model"),
            engine_kwargs={
                "tensor_parallel_size": 2,
                "pipeline_parallel_size": 2,
            },
            rank_to_bundle_index=[0, 0, 1, 1],  # Valid mapping
            deployment_config={
                "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
                "placement_group_strategy": "PACK",
            },
        )
        engine_config = config.get_engine_config()
        deployment_config = config.deployment_config.copy()

        # Should not raise any exception
        config._validate_user_placement_config(deployment_config, engine_config)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
