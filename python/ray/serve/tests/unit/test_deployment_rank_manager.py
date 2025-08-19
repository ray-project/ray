import pytest

from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.deployment_state import DeploymentRankManager


@pytest.fixture
def rank_manager():
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
        assert rank_manager._replica_ranks == {}
        assert rank_manager._available_ranks == set()
        assert rank_manager._next_rank == 0

    def test_assign_rank_first_replica(self, rank_manager):
        """Test assigning rank to first replica."""
        rank = rank_manager.assign_rank("replica_1")
        assert rank == 0
        assert rank_manager._replica_ranks["replica_1"] == 0
        assert rank_manager._next_rank == 1
        assert rank_manager._available_ranks == set()

    def test_assign_rank_multiple_replicas(self, rank_manager):
        """Test assigning ranks to multiple replicas."""
        rank1 = rank_manager.assign_rank("replica_1")
        rank2 = rank_manager.assign_rank("replica_2")
        rank3 = rank_manager.assign_rank("replica_3")

        assert rank1 == 0
        assert rank2 == 1
        assert rank3 == 2
        assert rank_manager._next_rank == 3
        assert len(rank_manager._replica_ranks) == 3

    def test_assign_rank_reuses_available_ranks(self, rank_manager):
        """Test that released ranks are reused before assigning new ones."""
        # Assign ranks to 3 replicas
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.assign_rank("replica_3")

        # Release middle rank
        rank_manager.release_rank("replica_2")
        assert 1 in rank_manager._available_ranks

        # New replica should get the released rank
        rank = rank_manager.assign_rank("replica_4")
        assert rank == 1
        assert 1 not in rank_manager._available_ranks

    def test_assign_rank_duplicate_fails(self):
        """Test assigning rank to replica that already has one fails when flag is enabled."""
        rank_manager = DeploymentRankManager()
        rank_manager.assign_rank("replica_1")

        with pytest.raises(RuntimeError, match="already has a rank assigned"):
            rank_manager.assign_rank("replica_1")

    def test_release_rank(self, rank_manager):
        """Test releasing a rank makes it available for reuse."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")

        rank_manager.release_rank("replica_1")

        assert "replica_1" not in rank_manager._replica_ranks
        assert 0 in rank_manager._available_ranks
        assert "replica_2" in rank_manager._replica_ranks

    def test_release_rank_nonexistent_replica(self):
        """Test releasing rank for non-existent replica is safe."""
        rank_manager = DeploymentRankManager()
        with pytest.raises(RuntimeError, match="has no rank assigned"):
            rank_manager.release_rank("nonexistent")

    def test_recover_rank_basic(self, rank_manager):
        """Test basic rank recovery."""
        rank_manager.recover_rank("replica_1", 5)

        assert rank_manager._replica_ranks["replica_1"] == 5
        assert rank_manager._next_rank == 6

    def test_recover_rank_updates_next_rank(self, rank_manager):
        """Test that recovering a high rank updates next_rank appropriately."""
        rank_manager.assign_rank("replica_1")  # Gets rank 0
        rank_manager.recover_rank("replica_2", 10)

        assert rank_manager._next_rank == 11

        # New replica should get rank 11
        rank = rank_manager.assign_rank("replica_3")
        assert rank == 11

    def test_recover_rank_removes_from_available(self, rank_manager):
        """Test that recovering a rank removes it from available ranks."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.release_rank("replica_1")  # Rank 0 becomes available

        assert 0 in rank_manager._available_ranks

        # Recover rank 0
        rank_manager.recover_rank("replica_3", 0)

        assert 0 not in rank_manager._available_ranks
        assert rank_manager._replica_ranks["replica_3"] == 0

    def test_recover_rank_duplicate_fails(self):
        """Test recovering rank for replica that already has one fails when flag is enabled."""
        rank_manager = DeploymentRankManager()
        rank_manager.assign_rank("replica_1")

        with pytest.raises(RuntimeError, match="already has a rank assigned"):
            rank_manager.recover_rank("replica_1", 5)

    def test_get_replica_rank_existing(self, rank_manager):
        """Test getting rank for existing replica."""
        rank_manager.assign_rank("replica_1")
        rank = rank_manager.get_replica_rank("replica_1")
        assert rank == 0

    def test_get_replica_rank_nonexistent_fails(self):
        """Test getting rank for non-existent replica fails when flag is enabled."""
        rank_manager = DeploymentRankManager()
        with pytest.raises(RuntimeError, match="has no rank assigned"):
            rank_manager.get_replica_rank("nonexistent")

    def test_get_replica_ranks_mapping(self, rank_manager):
        """Test getting copy of replica ranks mapping."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")

        mapping = rank_manager.get_replica_ranks_mapping()
        expected = {"replica_1": 0, "replica_2": 1}

        assert mapping == expected

        # Verify it's a copy
        mapping["replica_3"] = 2
        assert "replica_3" not in rank_manager._replica_ranks

    def test_clear(self, rank_manager):
        """Test clearing all rank data."""
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("replica_2")
        rank_manager.release_rank("replica_1")

        rank_manager.clear()

        assert rank_manager._replica_ranks == {}
        assert rank_manager._available_ranks == set()
        assert rank_manager._next_rank == 0

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

        # Manually set up non-contiguous ranks
        rank_manager._replica_ranks = {
            "replica_1": 0,
            "replica_2": 2,  # Gap at rank 1
            "replica_3": 3,
        }

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Should reassign some replicas to make ranks contiguous
        assert len(result) > 0

        # After reassignment, ranks should be contiguous
        final_ranks = sorted(rank_manager._replica_ranks.values())
        expected_ranks = [0, 1, 2]
        assert final_ranks == expected_ranks

    def test_minimal_reassignment_keeps_existing_when_possible(self, rank_manager):
        """Test that minimal reassignment keeps existing ranks when possible."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Set up ranks: 0, 2, 5, 7 (non-contiguous)
        rank_manager._replica_ranks = {
            "replica_1": 0,  # Should keep this
            "replica_2": 2,  # Should keep this
            "replica_3": 5,  # Should be reassigned to 1
            "replica_4": 7,  # Should be reassigned to 3
        }

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Verify minimal reassignment
        assert len(result) == 2  # Only 2 replicas should be reassigned
        reassigned_ids = {r.replica_id.unique_id for r in result}
        assert reassigned_ids == {"replica_3", "replica_4"}

        # Verify final ranks are contiguous
        final_ranks = sorted(rank_manager._replica_ranks.values())
        assert final_ranks == [0, 1, 2, 3]

        # Verify that replica_1 and replica_2 kept their original ranks
        assert rank_manager._replica_ranks["replica_1"] == 0
        assert rank_manager._replica_ranks["replica_2"] == 2

    def test_check_rank_consistency_unranked_replicas_fails_when_flag_enabled(self):
        """Test consistency check fails when active replicas have no ranks and flag is enabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=True)
        replica1 = MockDeploymentReplica("replica_1")

        with pytest.raises(
            RuntimeError, match="Controller rank system is in an invalid state"
        ):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_unranked_replicas_logs_when_flag_disabled(self):
        """Test consistency check only logs when active replicas have no ranks and flag is disabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=False)
        replica1 = MockDeploymentReplica("replica_1")

        # When flag is disabled, it logs error but still tries to proceed with reassignment
        # However, the reassignment will fail when trying to access ranks that don't exist
        result = rank_manager.check_rank_consistency_and_reassign_minimally([replica1])
        assert result == [replica1]

    def test_check_rank_consistency_stale_ranks_fails_when_flag_enabled(self):
        """Test consistency check fails when there are stale ranks and flag is enabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=True)
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("stale_replica")

        with pytest.raises(
            RuntimeError, match="Controller rank system is in an invalid state"
        ):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_stale_ranks_logs_when_flag_disabled(self):
        """Test consistency check only logs when there are stale ranks and flag is disabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=False)
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        rank_manager.assign_rank("replica_1")
        rank_manager.assign_rank("stale_replica")

        # When flag is disabled, it logs error but continues with reassignment
        # Since only replica_1 is active and has rank 0, no reassignment needed
        result = rank_manager.check_rank_consistency_and_reassign_minimally([replica1])
        assert result == []

    def test_check_rank_consistency_duplicate_ranks_fails_when_flag_enabled(self):
        """Test consistency check fails when there are duplicate ranks and flag is enabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=True)
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")

        # Manually create duplicate ranks (this should never happen in normal operation)
        rank_manager._replica_ranks = {"replica_1": 0, "replica_2": 0}  # Duplicate!

        with pytest.raises(
            RuntimeError, match="Controller rank system is in an invalid state"
        ):
            rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1, replica2]
            )

    def test_check_rank_consistency_duplicate_ranks_logs_when_flag_disabled(self):
        """Test consistency check only logs when there are duplicate ranks and flag is disabled."""
        rank_manager = DeploymentRankManager(_fail_on_error=False)
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")

        # Manually create duplicate ranks (this should never happen in normal operation)
        rank_manager._replica_ranks = {"replica_1": 0, "replica_2": 0}  # Duplicate!
        rank_manager._next_rank = 1

        # When flag is disabled, it logs error but still performs reassignment to fix the issue
        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2]
        )
        assert result == [replica2] or result == [replica1]


class TestDeploymentRankManagerIntegration:
    """Integration tests for more complex scenarios."""

    def test_scaling_up_and_down_scenario(self, rank_manager):
        """Test a realistic scaling scenario."""
        # Start with 3 replicas
        replicas = [MockDeploymentReplica(f"replica_{i}") for i in range(3)]
        for i, replica in enumerate(replicas):
            rank = rank_manager.assign_rank(replica.replica_id.unique_id)
            assert rank == i

        # Check consistency - should be fine
        result = rank_manager.check_rank_consistency_and_reassign_minimally(replicas)
        assert result == []

        # Scale down to 2 replicas (remove middle one)
        rank_manager.release_rank("replica_1")
        active_replicas = [replicas[0], replicas[2]]  # replica_0 and replica_2

        print(f"After release - available_ranks: {rank_manager._available_ranks}")
        print(f"After release - replica_ranks: {rank_manager._replica_ranks}")

        # Check consistency - should need reassignment due to gap
        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            active_replicas
        )
        assert len(result) == 1  # replica_2 should be reassigned from rank 2 to rank 1

        print(f"After reassignment - available_ranks: {rank_manager._available_ranks}")
        print(f"After reassignment - replica_ranks: {rank_manager._replica_ranks}")

        # Scale back up - new replica should reuse available rank
        new_replica = MockDeploymentReplica("replica_3")
        rank = rank_manager.assign_rank("replica_3")
        print(f"New replica got rank: {rank}")
        assert rank == 2  # Should reuse the available rank 2 (replica_2's old rank)

        # Final consistency check - should be fine now
        all_replicas = [replicas[0], replicas[2], new_replica]
        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            all_replicas
        )
        assert len(result) == 0  # No reassignment needed

        # After reassignment, ranks should be contiguous
        final_ranks = sorted(rank_manager._replica_ranks.values())
        assert final_ranks == [0, 1, 2]

    def test_controller_recovery_scenario(self, rank_manager):
        """Test controller recovery with existing replica ranks."""
        # Simulate controller startup with existing replicas
        rank_manager.recover_rank("replica_1", 0)
        rank_manager.recover_rank("replica_2", 2)
        rank_manager.recover_rank("replica_3", 1)

        # Create mock replicas
        replicas = [
            MockDeploymentReplica("replica_1"),
            MockDeploymentReplica("replica_2"),
            MockDeploymentReplica("replica_3"),
        ]

        # Check consistency - ranks should be reordered to be contiguous
        _ = rank_manager.check_rank_consistency_and_reassign_minimally(replicas)

        # Should have contiguous ranks 0, 1, 2
        final_ranks = sorted(rank_manager._replica_ranks.values())
        assert final_ranks == [0, 1, 2]

        # Next replica should get rank 3
        new_rank = rank_manager.assign_rank("replica_4")
        assert new_rank == 3

    def test_complex_reassignment_scenario(self, rank_manager):
        """Test complex reassignment with many gaps."""
        # Set up replicas with scattered ranks: 0, 3, 7, 10
        replicas = [MockDeploymentReplica(f"replica_{i}") for i in range(4)]
        rank_manager._replica_ranks = {
            "replica_0": 0,
            "replica_1": 3,
            "replica_2": 7,
            "replica_3": 10,
        }

        result = rank_manager.check_rank_consistency_and_reassign_minimally(replicas)

        # Should reassign to make contiguous: 0, 1, 2, 3
        final_ranks = sorted(rank_manager._replica_ranks.values())
        assert final_ranks == [0, 1, 2, 3]

        # Verify minimal reassignment - replica_0 should keep rank 0
        assert rank_manager._replica_ranks["replica_0"] == 0

        # The algorithm is more efficient - some replicas can keep their existing ranks if they're in target range
        # For ranks [0, 3, 7, 10] -> [0, 1, 2, 3], replica_0 keeps 0, replica_1 can keep 3->reordered to 3
        # But the algorithm should actually reassign to make them contiguous at [0, 1, 2, 3]
        assert len(result) == 2  # At least 2 replicas reassigned


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
