import pytest

from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.deployment_state import DeploymentRankManager
from ray.serve.schema import ReplicaRank


@pytest.fixture
def rank_manager() -> DeploymentRankManager:
    """Fixture providing a fresh DeploymentRankManager instance for each test."""
    return DeploymentRankManager()


class MockDeploymentReplica:
    """Mock replica for testing without heavy dependencies."""

    def __init__(
        self,
        replica_id: str,
        deployment_name: str = "test_deployment",
        app_name: str = "test_app",
    ):
        self.replica_id = ReplicaID(
            unique_id=replica_id,
            deployment_id=DeploymentID(name=deployment_name, app_name=app_name),
        )

    def __str__(self):
        return f"MockDeploymentReplica(replica_id={self.replica_id})"


class TestDeploymentRankManager:
    """Test cases for DeploymentRankManager."""

    def test_init(self, rank_manager):
        """Test initialization creates empty state."""
        assert rank_manager.get_replica_ranks_mapping() == {}

    def test_assign_rank_first_replica(self, rank_manager):
        """Test assigning rank to first replica."""
        rank = rank_manager.assign_rank("replica_1")
        assert rank.rank == 0
        assert rank_manager.has_replica_rank("replica_1")
        assert rank_manager.get_replica_rank("replica_1").rank == 0

    def test_assign_rank_multiple_replicas(self, rank_manager):
        """Test assigning ranks to multiple replicas."""
        rank1 = rank_manager.assign_rank("replica_1")
        rank2 = rank_manager.assign_rank("replica_2")
        rank3 = rank_manager.assign_rank("replica_3")

        assert rank1.rank == 0
        assert rank2.rank == 1
        assert rank3.rank == 2

        mapping = rank_manager.get_replica_ranks_mapping()
        assert len(mapping) == 3
        assert mapping == {"replica_1": 0, "replica_2": 1, "replica_3": 2}

    def test_assign_rank_reuses_released_ranks(self, rank_manager):
        """Test that released ranks are reused before assigning new ones."""
        # Assign ranks to 3 replicas
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.assign_rank("replica_3")

        # Release middle rank
        rank_manager.release_rank("replica_2")
        assert not rank_manager.has_replica_rank("replica_2")

        # New replica should get the released rank (1)
        rank = rank_manager.assign_rank("replica_4")
        assert rank.rank == 1
        assert rank_manager.get_replica_rank("replica_4").rank == 1

    def test_assign_rank_duplicate_fails(self):
        """Test assigning rank to replica that already has one fails."""
        rank_manager = DeploymentRankManager()
        rank_manager.assign_rank("replica_1")

        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.assign_rank("replica_1")

    def test_release_rank(self, rank_manager):
        """Test releasing a rank makes it available for reuse."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")

        rank_manager.release_rank("replica_1")

        assert not rank_manager.has_replica_rank("replica_1")
        assert rank_manager.has_replica_rank("replica_2")
        assert rank_manager.get_replica_rank("replica_2").rank == 1

    def test_release_rank_nonexistent_replica(self):
        """Test releasing rank for non-existent replica fails."""
        rank_manager = DeploymentRankManager()
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.release_rank("nonexistent")

    def test_recover_rank_basic(self, rank_manager):
        """Test basic rank recovery."""
        rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
        )

        assert rank_manager.has_replica_rank("replica_1")
        assert rank_manager.get_replica_rank("replica_1").rank == 5

    def test_recover_rank_updates_next_rank(self, rank_manager):
        """Test that recovering a high rank updates next_rank appropriately."""
        rank_manager.assign_rank("replica_1")  # Gets rank 0
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=10, node_rank=0, local_rank=0)
        )

        # New replica should get rank 11 (next available after 10)
        rank = rank_manager.assign_rank("replica_3")
        assert rank.rank == 11

        mapping = rank_manager.get_replica_ranks_mapping()
        assert mapping == {"replica_1": 0, "replica_2": 10, "replica_3": 11}

    def test_recover_rank_removes_from_available(self, rank_manager):
        """Test that recovering a rank removes it from available ranks."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.release_rank("replica_1")  # Rank 0 becomes available

        # Recover rank 0 for a new replica
        rank_manager.recover_rank(
            "replica_3", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

        # Verify replica_3 has rank 0
        assert rank_manager.has_replica_rank("replica_3")
        assert rank_manager.get_replica_rank("replica_3").rank == 0

        # Next assigned replica should get rank 2 (not 0, which is now taken)
        rank = rank_manager.assign_rank("replica_4")
        assert rank.rank == 2

    def test_recover_rank_duplicate_fails(self):
        """Test recovering rank for replica that already has one fails."""
        rank_manager = DeploymentRankManager()
        rank_manager.assign_rank("replica_1")

        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.recover_rank(
                "replica_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
            )

    def test_get_replica_rank_existing(self, rank_manager):
        """Test getting rank for existing replica."""
        rank_manager.assign_rank("replica_1")
        rank = rank_manager.get_replica_rank("replica_1")
        assert rank.rank == 0

    def test_get_replica_rank_nonexistent_fails(self):
        """Test getting rank for non-existent replica fails."""
        rank_manager = DeploymentRankManager()
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.get_replica_rank("nonexistent")

    def test_get_replica_ranks_mapping(self, rank_manager):
        """Test getting copy of replica ranks mapping."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")

        mapping = rank_manager.get_replica_ranks_mapping()
        expected = {"replica_1": 0, "replica_2": 1}

        assert mapping == expected

        # Verify it's a copy by modifying it
        mapping["replica_3"] = 2
        # Get a fresh mapping to verify the original wasn't changed
        fresh_mapping = rank_manager.get_replica_ranks_mapping()
        assert "replica_3" not in fresh_mapping
        assert fresh_mapping == expected

    def test_clear(self, rank_manager):
        """Test clearing all rank data."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.release_rank("replica_1")

        rank_manager.clear()

        # After clearing, should have no ranks
        assert rank_manager.get_replica_ranks_mapping() == {}
        assert not rank_manager.has_replica_rank("replica_1")
        assert not rank_manager.has_replica_rank("replica_2")

        # Should be able to assign from 0 again
        rank = rank_manager.assign_rank("replica_3")
        assert rank.rank == 0

    def test_check_rank_consistency_empty_replicas(self, rank_manager):
        """Test consistency check with no active replicas."""
        result = rank_manager.check_rank_consistency_and_reassign_minimally([])
        assert result == []

    def test_check_rank_consistency_contiguous_ranks(self, rank_manager):
        """Test consistency check with contiguous ranks (no reassignment needed)."""
        # Set up contiguous ranks
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        rank_manager.assign_rank("replica_1")  # rank 0
        rank_manager.assign_rank("replica_2")  # rank 1
        rank_manager.assign_rank("replica_3")  # rank 2

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        assert result == []

    def test_check_rank_consistency_non_contiguous_ranks(self, rank_manager):
        """Test consistency check with non-contiguous ranks (reassignment needed)."""
        # Set up non-contiguous ranks (simulate a replica being removed)
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        # Manually assign non-contiguous ranks using recover_rank
        rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=2, node_rank=0, local_rank=0)
        )  # Gap at rank 1
        rank_manager.recover_rank(
            "replica_3", ReplicaRank(rank=3, node_rank=0, local_rank=0)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Should reassign some replicas to make ranks contiguous
        assert len(result) > 0

        # After reassignment, ranks should be contiguous [0, 1, 2]
        mapping = rank_manager.get_replica_ranks_mapping()
        final_ranks = sorted(mapping.values())
        expected_ranks = [0, 1, 2]
        assert final_ranks == expected_ranks

    def test_minimal_reassignment_keeps_existing_when_possible(self, rank_manager):
        """Test that minimal reassignment keeps existing ranks when possible."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Set up ranks: 0, 2, 5, 7 (non-contiguous) using recover_rank
        rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )  # Should keep this
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=2, node_rank=0, local_rank=0)
        )  # Should keep this
        rank_manager.recover_rank(
            "replica_3", ReplicaRank(rank=5, node_rank=0, local_rank=0)
        )  # Should be reassigned to 1
        rank_manager.recover_rank(
            "replica_4", ReplicaRank(rank=7, node_rank=0, local_rank=0)
        )  # Should be reassigned to 3

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Verify minimal reassignment
        assert len(result) == 2  # Only 2 replicas should be reassigned
        reassigned_ids = {r.replica_id.unique_id for r in result}
        assert reassigned_ids == {"replica_3", "replica_4"}

        # Verify final ranks are contiguous
        mapping = rank_manager.get_replica_ranks_mapping()
        final_ranks = sorted(mapping.values())
        assert final_ranks == [0, 1, 2, 3]

        # Verify that replica_1 and replica_2 kept their original ranks
        assert rank_manager.get_replica_rank("replica_1").rank == 0
        assert rank_manager.get_replica_rank("replica_2").rank == 2

    def test_check_rank_consistency_unranked_replicas_fails(self):
        """Test consistency check fails when active replicas have no ranks."""
        rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_stale_ranks_fails(self):
        """Test consistency check fails when there are stale ranks."""
        rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("stale_replica")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_duplicate_ranks_fails(self):
        """Test consistency check fails when there are duplicate ranks."""
        rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")

        # Manually create duplicate ranks using recover_rank (this should never happen in normal operation)
        rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )  # Duplicate!

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1, replica2]
            )


class TestDeploymentRankManagerErrorHandling:
    """Test cases for DeploymentRankManager error handling with fail_on_rank_error flag.

    This test class can be easily removed in the future when the error handling
    feature is no longer needed.
    """

    def test_assign_rank_error_with_fail_on_rank_error_true(self):
        """Test that assign_rank raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)
        rank_manager.assign_rank("replica_1")

        # Should raise RuntimeError for duplicate assignment
        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.assign_rank("replica_1")

    def test_assign_rank_error_with_fail_on_rank_error_false(self):
        """Test that assign_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        rank_manager.assign_rank("replica_1")

        # Should return safe default (ReplicaRank(rank=0)) instead of raising
        result = rank_manager.assign_rank("replica_1")
        assert result is not None
        assert isinstance(result, ReplicaRank)
        assert result.rank == 0

    def test_release_rank_error_with_fail_on_rank_error_true(self):
        """Test that release_rank raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)

        # Should raise RuntimeError for non-existent replica
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.release_rank("nonexistent")

    def test_release_rank_error_with_fail_on_rank_error_false(self):
        """Test that release_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)

        # Should return None instead of raising
        result = rank_manager.release_rank("nonexistent")
        assert result is None

    def test_recover_rank_error_with_fail_on_rank_error_true(self):
        """Test that recover_rank raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)
        rank_manager.assign_rank("replica_1")

        # Should raise RuntimeError for duplicate recovery
        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.recover_rank(
                "replica_1", ReplicaRank(rank=5, node_rank=-1, local_rank=-1)
            )

    def test_recover_rank_error_with_fail_on_rank_error_false(self):
        """Test that recover_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        rank_manager.assign_rank("replica_1")

        # Should return None instead of raising
        result = rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=5, node_rank=-1, local_rank=-1)
        )
        assert result is None

    def test_get_replica_rank_error_with_fail_on_rank_error_true(self):
        """Test that get_replica_rank raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)

        # Should raise RuntimeError for non-existent replica
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.get_replica_rank("nonexistent")

    def test_get_replica_rank_error_with_fail_on_rank_error_false(self):
        """Test that get_replica_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)

        # Should return safe default (ReplicaRank(rank=0)) instead of raising
        result = rank_manager.get_replica_rank("nonexistent")
        assert result is not None
        assert isinstance(result, ReplicaRank)
        assert result.rank == 0

    def test_check_rank_consistency_error_with_fail_on_rank_error_true(self):
        """Test that check_rank_consistency raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)
        replica1 = MockDeploymentReplica("replica_1")

        # Set up invalid state: active replica without rank
        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_error_with_fail_on_rank_error_false(self):
        """Test that check_rank_consistency returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        replica1 = MockDeploymentReplica("replica_1")

        # Should return empty list instead of raising
        result = rank_manager.check_rank_consistency_and_reassign_minimally([replica1])
        assert result == []

    def test_check_rank_consistency_with_stale_ranks_error_handling(self):
        """Test check_rank_consistency with stale ranks and fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("stale_replica")

        # Should return empty list instead of raising
        result = rank_manager.check_rank_consistency_and_reassign_minimally([replica1])
        assert result == []

    def test_check_rank_consistency_with_duplicate_ranks_error_handling(self):
        """Test check_rank_consistency with duplicate ranks and fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")

        # Manually create duplicate ranks
        rank_manager.recover_rank(
            "replica_1", ReplicaRank(rank=0, node_rank=-1, local_rank=-1)
        )
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=0, node_rank=-1, local_rank=-1)
        )

        # Should return empty list instead of raising
        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2]
        )
        assert result == []

    def test_normal_operations_work_with_fail_on_rank_error_false(self):
        """Test that normal operations still work correctly with fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)

        # Test normal assign
        rank1 = rank_manager.assign_rank("replica_1")
        assert rank1.rank == 0

        # Test normal get
        rank = rank_manager.get_replica_rank("replica_1")
        assert rank.rank == 0

        # Test normal release
        rank_manager.release_rank("replica_1")
        assert not rank_manager.has_replica_rank("replica_1")

        # Test normal recover
        rank_manager.recover_rank(
            "replica_2", ReplicaRank(rank=5, node_rank=-1, local_rank=-1)
        )

        assert rank_manager.get_replica_rank("replica_2").rank == 5

        # Test normal consistency check
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        rank_manager.assign_rank("replica_3")

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica2, replica3]
        )
        # Should reassign to make ranks contiguous
        assert len(result) > 0

    def test_multiple_errors_do_not_crash_with_fail_on_rank_error_false(self):
        """Test that multiple consecutive errors don't crash when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)

        # Multiple errors in a row should all return safe defaults
        result1 = rank_manager.get_replica_rank("nonexistent1")
        result2 = rank_manager.get_replica_rank("nonexistent2")
        result3 = rank_manager.release_rank("nonexistent3")

        assert result1 is not None
        assert result2 is not None
        assert result3 is None

        # And normal operations should still work after errors
        rank = rank_manager.assign_rank("replica_1")
        assert rank.rank == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
