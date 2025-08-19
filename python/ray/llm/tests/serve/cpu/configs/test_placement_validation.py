import sys
from unittest.mock import Mock

import pytest


class TestPlacementGroupValidation:
    """Test placement group validation logic without Ray runtime dependencies."""

    class MockLLMConfig:
        def __init__(self, rank_to_bundle_index=None):
            self.rank_to_bundle_index = rank_to_bundle_index

        def _validate_user_placement_config(self, deployment_config, engine_config):
            """Copy of our validation method."""
            # Both bundles and strategy must be provided together
            if "placement_group_bundles" not in deployment_config:
                raise ValueError(
                    "placement_group_strategy specified without placement_group_bundles. "
                    "Both must be provided for custom placement group configuration."
                )
            if "placement_group_strategy" not in deployment_config:
                raise ValueError(
                    "placement_group_bundles specified without placement_group_strategy. "
                    "Both must be provided for custom placement group configuration."
                )

            bundles = deployment_config["placement_group_bundles"]
            strategy = deployment_config["placement_group_strategy"]

            # Validate strategy
            valid_strategies = {"PACK", "SPREAD", "STRICT_PACK", "STRICT_SPREAD"}
            if strategy not in valid_strategies:
                raise ValueError(
                    f"Invalid placement_group_strategy: {strategy}. "
                    f"Valid options are: {valid_strategies}"
                )

            # Validate bundles
            if not bundles or not isinstance(bundles, list):
                raise ValueError("placement_group_bundles must be a non-empty list")

            total_gpus = 0
            for i, bundle in enumerate(bundles):
                if not isinstance(bundle, dict):
                    raise ValueError(f"Bundle {i} must be a dictionary")

                # Check for negative resources
                for resource, amount in bundle.items():
                    if not isinstance(amount, (int, float)) or amount < 0:
                        raise ValueError(
                            f"Bundle {i} resource '{resource}' must be a non-negative number, got {amount}"
                        )

                total_gpus += bundle.get("GPU", 0)

            # Get expected GPU count from engine config and validate rank mapping
            try:
                tp_size = getattr(engine_config, "tensor_parallel_degree", 1)
                pp_size = getattr(engine_config, "pipeline_parallel_degree", 1)
                expected_gpus = tp_size * pp_size

                if total_gpus < expected_gpus:
                    raise ValueError(
                        f"Insufficient GPU resources in placement group bundles. "
                        f"Need at least {expected_gpus} GPUs for tensor_parallel_size={tp_size}, "
                        f"pipeline_parallel_size={pp_size}, but got {total_gpus} GPUs total."
                    )

                # Validate rank_to_bundle_index if provided
                if self.rank_to_bundle_index is not None:
                    if len(self.rank_to_bundle_index) != expected_gpus:
                        raise ValueError(
                            f"rank_to_bundle_index must have length {expected_gpus} "
                            f"(tensor_parallel_size * pipeline_parallel_size), "
                            f"but got {len(self.rank_to_bundle_index)}"
                        )

                    max_bundle_index = len(bundles) - 1
                    for rank, bundle_idx in enumerate(self.rank_to_bundle_index):
                        if (
                            not isinstance(bundle_idx, int)
                            or bundle_idx < 0
                            or bundle_idx > max_bundle_index
                        ):
                            raise ValueError(
                                f"rank_to_bundle_index[{rank}] = {bundle_idx} is invalid. "
                                f"Must be an integer between 0 and {max_bundle_index} (inclusive)."
                            )

            except AttributeError:
                # If engine config doesn't have parallelism info, skip this validation
                pass

    def test_missing_strategy(self):
        config = self.MockLLMConfig()
        deployment_config = {"placement_group_bundles": [{"GPU": 1}]}
        engine_config = Mock()

        with pytest.raises(
            ValueError,
            match="placement_group_bundles specified without placement_group_strategy",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_missing_bundles(self):
        config = self.MockLLMConfig()
        deployment_config = {"placement_group_strategy": "PACK"}
        engine_config = Mock()

        with pytest.raises(
            ValueError,
            match="placement_group_strategy specified without placement_group_bundles",
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_strategy(self):
        config = self.MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"GPU": 1}],
            "placement_group_strategy": "INVALID",
        }
        engine_config = Mock()

        with pytest.raises(
            ValueError, match="Invalid placement_group_strategy: INVALID"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_empty_bundles(self):
        config = self.MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [],
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()

        with pytest.raises(
            ValueError, match="placement_group_bundles must be a non-empty list"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_non_dict_bundle(self):
        config = self.MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": ["not_a_dict"],
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()

        with pytest.raises(ValueError, match="Bundle 0 must be a dictionary"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_negative_resources(self):
        config = self.MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"GPU": -1}],
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()

        with pytest.raises(
            ValueError, match="Bundle 0 resource 'GPU' must be a non-negative number"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_insufficient_gpu_resources(self):
        config = self.MockLLMConfig()
        deployment_config = {
            "placement_group_bundles": [{"GPU": 2}],  # Only 2 GPUs
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 4
        engine_config.pipeline_parallel_degree = 2  # Need 8 GPUs total

        with pytest.raises(ValueError, match="Insufficient GPU resources"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_rank_to_bundle_index_length(self):
        config = self.MockLLMConfig(rank_to_bundle_index=[0, 1])  # Wrong length
        deployment_config = {
            "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 2
        engine_config.pipeline_parallel_degree = 2  # Need 4 ranks

        with pytest.raises(ValueError, match="rank_to_bundle_index must have length 4"):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_invalid_rank_to_bundle_index_value(self):
        config = self.MockLLMConfig(
            rank_to_bundle_index=[0, 5]
        )  # Bundle index 5 doesn't exist
        deployment_config = {
            "placement_group_bundles": [
                {"GPU": 1},
                {"GPU": 1},
            ],  # Only 2 bundles (indices 0,1)
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 2
        engine_config.pipeline_parallel_degree = 1  # 2 ranks total

        with pytest.raises(
            ValueError, match="rank_to_bundle_index\\[1\\] = 5 is invalid"
        ):
            config._validate_user_placement_config(deployment_config, engine_config)

    def test_valid_configuration(self):
        config = self.MockLLMConfig(rank_to_bundle_index=[0, 0, 1, 1])
        deployment_config = {
            "placement_group_bundles": [{"GPU": 2}, {"GPU": 2}],
            "placement_group_strategy": "PACK",
        }
        engine_config = Mock()
        engine_config.tensor_parallel_degree = 2
        engine_config.pipeline_parallel_degree = 2  # 4 ranks total

        # Should not raise any exception
        config._validate_user_placement_config(deployment_config, engine_config)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
