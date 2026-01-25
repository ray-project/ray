"""Unit tests for StaticPlacementConfig."""
import pytest

from ray.serve.config import StaticPlacementConfig


class FakePlacementGroup:
    """Fake placement group for unit testing."""

    def __init__(self, name="fake_pg", num_bundles=4):
        self.name = name
        # Simulate bundle_specs as a list of bundle resource dicts
        self.bundle_specs = [{"CPU": 1}] * num_bundles


class TestStaticPlacementConfig:
    """Test suite for StaticPlacementConfig dataclass."""

    def test_basic_creation(self):
        """Test basic creation of StaticPlacementConfig."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={
                0: [0],
                1: [1],
            },
        )
        assert config.placement_group == pg
        assert config.num_replicas == 2
        assert config.capture_child_tasks is True

    def test_capture_child_tasks_false(self):
        """Test capture_child_tasks can be set to False."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={0: [0]},
            capture_child_tasks=False,
        )
        assert config.capture_child_tasks is False

    def test_multiple_bundles_per_replica(self):
        """Test that replicas can have multiple bundles."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={
                0: [0, 1],
                1: [2, 3],
            },
        )
        assert config.num_replicas == 2
        assert config.get_bundle_indices_for_replica(0) == [0, 1]
        assert config.get_bundle_indices_for_replica(1) == [2, 3]
        assert config.get_primary_bundle_index_for_replica(0) == 0
        assert config.get_primary_bundle_index_for_replica(1) == 2

    def test_empty_mapping_raises_error(self):
        """Test that empty mapping raises ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(ValueError, match="replica_bundle_mapping cannot be empty"):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={},
            )

    def test_non_contiguous_ranks_raises_error(self):
        """Test that non-contiguous ranks raise ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(ValueError, match="Replica ranks must be contiguous"):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: [0],
                    2: [1],  # Missing rank 1
                },
            )

    def test_non_zero_starting_rank_raises_error(self):
        """Test that ranks not starting from 0 raise ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(ValueError, match="Replica ranks must be contiguous"):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    1: [0],
                    2: [1],
                },
            )

    def test_empty_bundle_list_raises_error(self):
        """Test that empty bundle list raises ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(
            ValueError, match="must be mapped to at least one bundle index"
        ):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: [],  # Empty list
                },
            )

    def test_negative_bundle_index_raises_error(self):
        """Test that negative bundle indices raise ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(
            ValueError, match="Bundle indices.*must be non-negative integers"
        ):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: [-1],
                },
            )

    def test_non_integer_bundle_index_raises_error(self):
        """Test that non-integer bundle indices raise ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(
            ValueError, match="Bundle indices.*must be non-negative integers"
        ):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: ["invalid"],  # type: ignore
                },
            )

    def test_duplicate_bundle_indices_raises_error(self):
        """Test that duplicate bundle indices across replicas raise ValueError."""
        pg = FakePlacementGroup()
        with pytest.raises(ValueError, match="Bundle indices must be unique"):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: [0, 1],
                    1: [1, 2],  # Bundle 1 is duplicated
                },
            )

    def test_bundle_index_out_of_bounds_raises_error(self):
        """Test that bundle indices exceeding placement group size raise ValueError."""
        pg = FakePlacementGroup(num_bundles=2)  # Only bundles 0 and 1 exist
        with pytest.raises(ValueError, match="Bundle index 2 is out of bounds"):
            StaticPlacementConfig(
                placement_group=pg,
                replica_bundle_mapping={
                    0: [0],
                    1: [2],  # Bundle 2 doesn't exist
                },
            )

    def test_get_bundle_indices_for_invalid_rank_raises_error(self):
        """Test that getting bundle indices for invalid rank raises KeyError."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={0: [0]},
        )
        with pytest.raises(KeyError, match="Replica rank 1 not found in mapping"):
            config.get_bundle_indices_for_replica(1)

    def test_num_replicas_property(self):
        """Test num_replicas property returns correct count."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={
                0: [0],
                1: [1],
                2: [2],
            },
        )
        assert config.num_replicas == 3

    def test_single_replica_single_bundle(self):
        """Test simple case of single replica with single bundle."""
        pg = FakePlacementGroup()
        config = StaticPlacementConfig(
            placement_group=pg,
            replica_bundle_mapping={0: [0]},
        )
        assert config.num_replicas == 1
        assert config.get_bundle_indices_for_replica(0) == [0]
        assert config.get_primary_bundle_index_for_replica(0) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
