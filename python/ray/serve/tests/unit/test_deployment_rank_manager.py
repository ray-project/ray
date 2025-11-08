import pytest

from ray.serve._private.common import DeploymentID, ReplicaID
from ray.serve._private.deployment_state import DeploymentRankManager, RankManager


@pytest.fixture
def rank_manager():
    """Fixture providing a fresh RankManager instance for each test."""
    return RankManager()


@pytest.fixture
def deployment_rank_manager():
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


class TestRankManager:
    """Test cases for the base RankManager class."""

    def test_init(self, rank_manager):
        """Test initialization creates empty state."""
        assert rank_manager.get_ranks_mapping() == {}
        assert rank_manager._released_ranks == set()
        assert rank_manager._next_rank == 0

    def test_assign_rank_first_key(self, rank_manager):
        """Test assigning rank to first key."""
        rank = rank_manager.assign_rank("key_1")
        assert rank == 0
        assert rank_manager.get_rank("key_1") == 0
        assert rank_manager._next_rank == 1

    def test_assign_rank_multiple_keys(self, rank_manager):
        """Test assigning ranks to multiple keys."""
        rank1 = rank_manager.assign_rank("key_1")
        rank2 = rank_manager.assign_rank("key_2")
        rank3 = rank_manager.assign_rank("key_3")

        assert rank1 == 0
        assert rank2 == 1
        assert rank3 == 2
        assert rank_manager._next_rank == 3

    def test_assign_rank_duplicate_fails(self, rank_manager):
        """Test assigning rank to key that already has one fails."""
        rank_manager.assign_rank("key_1")

        with pytest.raises(RuntimeError, match="Rank for .* already assigned"):
            rank_manager.assign_rank("key_1")

    def test_release_rank(self, rank_manager):
        """Test releasing a rank."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("key_2")

        rank_manager.release_rank("key_1")

        assert "key_1" not in rank_manager.get_ranks_mapping()
        assert "key_2" in rank_manager.get_ranks_mapping()
        assert 0 in rank_manager._released_ranks

    def test_release_rank_nonexistent_fails(self, rank_manager):
        """Test releasing rank for non-existent key fails."""
        with pytest.raises(RuntimeError, match="Rank for .* not assigned"):
            rank_manager.release_rank("nonexistent")

    def test_rank_reuse_after_release(self, rank_manager):
        """Test that released ranks are reused in ascending order."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("key_2")
        rank_manager.assign_rank("key_3")

        # Release key_2 (rank 1)
        rank_manager.release_rank("key_2")

        # New key should get the released rank
        rank4 = rank_manager.assign_rank("key_4")
        assert rank4 == 1

    def test_rank_reuse_multiple_released(self, rank_manager):
        """Test that multiple released ranks are reused in order (min first)."""
        # Assign ranks 0-4
        for i in range(5):
            rank_manager.assign_rank(f"key_{i}")

        # Release ranks 1, 3, 2 (in that order)
        rank_manager.release_rank("key_1")  # Release rank 1
        rank_manager.release_rank("key_3")  # Release rank 3
        rank_manager.release_rank("key_2")  # Release rank 2

        # Should reuse in ascending order: 1, 2, 3
        rank1 = rank_manager.assign_rank("key_new1")
        rank2 = rank_manager.assign_rank("key_new2")
        rank3 = rank_manager.assign_rank("key_new3")

        assert rank1 == 1
        assert rank2 == 2
        assert rank3 == 3

    def test_recover_rank(self, rank_manager):
        """Test basic rank recovery."""
        rank_manager.recover_rank("key_1", 5)

        assert rank_manager.get_rank("key_1") == 5
        assert rank_manager._next_rank == 6

    def test_recover_rank_updates_next_rank(self, rank_manager):
        """Test that recovering a high rank updates next_rank appropriately."""
        rank_manager.assign_rank("key_1")  # Gets rank 0
        rank_manager.recover_rank("key_2", 10)

        # New key should get rank 11
        rank3 = rank_manager.assign_rank("key_3")
        assert rank3 == 11

    def test_recover_rank_removes_from_released(self, rank_manager):
        """Test that recovering a rank removes it from released ranks."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("key_2")
        rank_manager.release_rank("key_1")  # Rank 0 becomes available

        # Recover rank 0 for key_3
        rank_manager.recover_rank("key_3", 0)
        assert rank_manager.get_rank("key_3") == 0

        # Rank 0 should not be reused
        rank4 = rank_manager.assign_rank("key_4")
        assert rank4 == 2  # Not 0

    def test_recover_rank_duplicate_fails(self, rank_manager):
        """Test recovering rank for key that already has one fails."""
        rank_manager.assign_rank("key_1")

        with pytest.raises(RuntimeError, match="Rank for .* already assigned"):
            rank_manager.recover_rank("key_1", 5)

    def test_get_rank(self, rank_manager):
        """Test getting rank for existing key."""
        rank_manager.assign_rank("key_1")
        assert rank_manager.get_rank("key_1") == 0

    def test_get_rank_nonexistent_fails(self, rank_manager):
        """Test getting rank for non-existent key fails."""
        with pytest.raises(RuntimeError, match="Rank for .* not assigned"):
            rank_manager.get_rank("nonexistent")

    def test_has_rank(self, rank_manager):
        """Test checking if key has rank."""
        assert not rank_manager.has_rank("key_1")

        rank_manager.assign_rank("key_1")
        assert rank_manager.has_rank("key_1")

        rank_manager.release_rank("key_1")
        assert not rank_manager.has_rank("key_1")

    def test_get_ranks_mapping(self, rank_manager):
        """Test getting copy of ranks mapping."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("key_2")

        mapping = rank_manager.get_ranks_mapping()
        expected = {"key_1": 0, "key_2": 1}

        assert mapping == expected

        # Verify it's a copy
        mapping["key_3"] = 2
        new_mapping = rank_manager.get_ranks_mapping()
        assert "key_3" not in new_mapping

    def test_clear(self, rank_manager):
        """Test clearing all rank data."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("key_2")
        rank_manager.release_rank("key_1")

        rank_manager.clear()

        assert rank_manager.get_ranks_mapping() == {}
        assert rank_manager._released_ranks == set()
        assert rank_manager._next_rank == 0

        # Verify next assigned rank starts from 0
        rank = rank_manager.assign_rank("key_3")
        assert rank == 0

    def test_check_consistency_empty(self, rank_manager):
        """Test consistency check with no active keys."""
        result = rank_manager.check_rank_consistency_and_reassign_minimally([])
        assert result == []

    def test_check_consistency_contiguous_ranks(self, rank_manager):
        """Test consistency check with contiguous ranks (no reassignment needed)."""
        rank_manager.assign_rank("key_1")  # rank 0
        rank_manager.assign_rank("key_2")  # rank 1
        rank_manager.assign_rank("key_3")  # rank 2

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            ["key_1", "key_2", "key_3"]
        )

        assert result == []

    def test_check_consistency_non_contiguous_ranks(self, rank_manager):
        """Test consistency check with non-contiguous ranks (reassignment needed)."""
        rank_manager.assign_rank("key_1")  # rank 0
        rank_manager.assign_rank("key_temp")  # rank 1
        rank_manager.assign_rank("key_2")  # rank 2
        rank_manager.assign_rank("key_3")  # rank 3
        rank_manager.release_rank("key_temp")  # Gap at 1

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            ["key_1", "key_2", "key_3"]
        )

        # Some keys should be reassigned
        assert len(result) > 0

        # After reassignment, ranks should be [0, 1, 2]
        final_ranks = sorted(rank_manager.get_ranks_mapping().values())
        assert final_ranks == [0, 1, 2]

    def test_check_consistency_minimal_reassignment(self, rank_manager):
        """Test that consistency check performs minimal reassignment."""
        # Set up ranks: 0, 2, 5, 7 (non-contiguous)
        rank_manager.recover_rank("key_1", 0)  # Should keep this
        rank_manager.recover_rank("key_2", 2)  # Should keep this
        rank_manager.recover_rank("key_3", 5)  # Should be reassigned to 1
        rank_manager.recover_rank("key_4", 7)  # Should be reassigned to 3

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            ["key_1", "key_2", "key_3", "key_4"]
        )

        # Only 2 keys should be reassigned
        assert len(result) == 2
        assert set(result) == {"key_3", "key_4"}

        # Verify that key_1 and key_2 kept their ranks
        mapping = rank_manager.get_ranks_mapping()
        assert mapping["key_1"] == 0
        assert mapping["key_2"] == 2

    def test_check_consistency_unranked_keys_fails(self, rank_manager):
        """Test consistency check fails when active keys have no ranks."""
        rank_manager.assign_rank("key_1")

        # key_2 doesn't have a rank
        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally(
                ["key_1", "key_2"]
            )

    def test_check_consistency_stale_ranks_fails(self, rank_manager):
        """Test consistency check fails when there are stale ranks."""
        rank_manager.assign_rank("key_1")
        rank_manager.assign_rank("stale_key")

        # stale_key is not in active list
        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally(["key_1"])

    def test_check_consistency_duplicate_ranks_fails(self, rank_manager):
        """Test consistency check fails when there are duplicate ranks."""
        # Manually create duplicate ranks (should never happen in normal operation)
        rank_manager._ranks = {"key_1": 0, "key_2": 0}

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally(
                ["key_1", "key_2"]
            )

    def test_perform_minimal_rank_reassignment(self, rank_manager):
        """Test the minimal rank reassignment algorithm."""
        # Set up non-contiguous ranks
        rank_manager.recover_rank("key_1", 0)
        rank_manager.recover_rank("key_2", 2)
        rank_manager.recover_rank("key_3", 5)

        result = rank_manager._perform_minimal_rank_reassignment(
            ["key_1", "key_2", "key_3"]
        )

        # key_3 should be reassigned (5 -> 1)
        assert "key_3" in result
        # key_1 and key_2 should keep their ranks
        assert "key_1" not in result
        assert "key_2" not in result

        # Verify final ranks are contiguous
        mapping = rank_manager.get_ranks_mapping()
        assert mapping["key_1"] == 0
        assert mapping["key_2"] == 2
        assert mapping["key_3"] == 1


class TestDeploymentRankManagerBasic:
    """Test cases for basic DeploymentRankManager operations."""

    def test_init(self, deployment_rank_manager):
        """Test initialization creates empty state."""
        assert deployment_rank_manager._replica_rank_manager.get_ranks_mapping() == {}
        assert deployment_rank_manager._node_rank_manager.get_ranks_mapping() == {}
        assert deployment_rank_manager._local_rank_managers == {}
        assert deployment_rank_manager._replica_to_node == {}

    def test_assign_rank_single_node_single_replica(self, deployment_rank_manager):
        """Test assigning rank to first replica on a node."""
        global_rank, node_rank, local_rank = deployment_rank_manager.assign_rank(
            "replica_1", "node_1"
        )

        assert global_rank == 0
        assert node_rank == 0
        assert local_rank == 0

        # Verify ranks can be retrieved
        ranks = deployment_rank_manager.get_replica_rank("replica_1")
        assert ranks == (0, 0, 0)

    def test_assign_rank_single_node_multiple_replicas(self, deployment_rank_manager):
        """Test assigning ranks to multiple replicas on the same node."""
        global_rank1, node_rank1, local_rank1 = deployment_rank_manager.assign_rank(
            "replica_1", "node_1"
        )
        global_rank2, node_rank2, local_rank2 = deployment_rank_manager.assign_rank(
            "replica_2", "node_1"
        )
        global_rank3, node_rank3, local_rank3 = deployment_rank_manager.assign_rank(
            "replica_3", "node_1"
        )

        # Global ranks are unique across all replicas
        assert global_rank1 == 0
        assert global_rank2 == 1
        assert global_rank3 == 2

        # Node rank is the same (same node)
        assert node_rank1 == 0
        assert node_rank2 == 0
        assert node_rank3 == 0

        # Local ranks are unique within the node
        assert local_rank1 == 0
        assert local_rank2 == 1
        assert local_rank3 == 2

    def test_assign_rank_multiple_nodes(self, deployment_rank_manager):
        """Test assigning ranks to replicas across multiple nodes."""
        # Node 1: 2 replicas
        global_rank1, node_rank1, local_rank1 = deployment_rank_manager.assign_rank(
            "replica_1", "node_1"
        )
        global_rank2, node_rank2, local_rank2 = deployment_rank_manager.assign_rank(
            "replica_2", "node_1"
        )

        # Node 2: 2 replicas
        global_rank3, node_rank3, local_rank3 = deployment_rank_manager.assign_rank(
            "replica_3", "node_2"
        )
        global_rank4, node_rank4, local_rank4 = deployment_rank_manager.assign_rank(
            "replica_4", "node_2"
        )

        # Node 3: 1 replica
        global_rank5, node_rank5, local_rank5 = deployment_rank_manager.assign_rank(
            "replica_5", "node_3"
        )

        # Global ranks are unique
        assert global_rank1 == 0
        assert global_rank2 == 1
        assert global_rank3 == 2
        assert global_rank4 == 3
        assert global_rank5 == 4

        # Node ranks are assigned in order
        assert node_rank1 == 0
        assert node_rank2 == 0
        assert node_rank3 == 1
        assert node_rank4 == 1
        assert node_rank5 == 2

        # Local ranks reset per node
        assert local_rank1 == 0
        assert local_rank2 == 1
        assert local_rank3 == 0
        assert local_rank4 == 1
        assert local_rank5 == 0

    def test_assign_rank_duplicate_replica_fails(self, deployment_rank_manager):
        """Test assigning rank to replica that already has one fails."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")

        with pytest.raises(RuntimeError, match="Rank for .* already assigned"):
            deployment_rank_manager.assign_rank("replica_1", "node_1")

    def test_release_rank_single_replica(self, deployment_rank_manager):
        """Test releasing a rank for a single replica."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")

        deployment_rank_manager.release_rank("replica_1")

        # Verify replica_1 is no longer tracked
        assert not deployment_rank_manager.has_replica_rank("replica_1")
        assert deployment_rank_manager.has_replica_rank("replica_2")

    def test_release_rank_last_replica_on_node(self, deployment_rank_manager):
        """Test that releasing the last replica on a node also releases the node rank."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_2")

        # Release the only replica on node_1
        deployment_rank_manager.release_rank("replica_1")

        # Node 1 should no longer have a rank manager
        assert "node_1" not in deployment_rank_manager._local_rank_managers

        # Node 1 should no longer have a node rank
        assert not deployment_rank_manager._node_rank_manager.has_rank("node_1")

        # Node 2 should still be intact
        assert "node_2" in deployment_rank_manager._local_rank_managers
        assert deployment_rank_manager._node_rank_manager.has_rank("node_2")

    def test_release_rank_nonexistent_replica_fails(self, deployment_rank_manager):
        """Test releasing rank for non-existent replica fails."""
        with pytest.raises(RuntimeError, match="Rank for .* not assigned"):
            deployment_rank_manager.release_rank("nonexistent")

    def test_recover_rank_single_replica(self, deployment_rank_manager):
        """Test basic rank recovery for a single replica."""
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=5, local_rank=2, node_rank=1
        )

        ranks = deployment_rank_manager.get_replica_rank("replica_1")
        assert ranks == (5, 1, 2)

    def test_recover_rank_multiple_replicas_same_node(self, deployment_rank_manager):
        """Test recovering multiple replicas on the same node."""
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=0, local_rank=0, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_2", "node_1", global_rank=1, local_rank=1, node_rank=0
        )

        ranks1 = deployment_rank_manager.get_replica_rank("replica_1")
        ranks2 = deployment_rank_manager.get_replica_rank("replica_2")

        assert ranks1 == (0, 0, 0)
        assert ranks2 == (1, 0, 1)

        # Both should be on the same node
        assert deployment_rank_manager._replica_to_node["replica_1"] == "node_1"
        assert deployment_rank_manager._replica_to_node["replica_2"] == "node_1"

    def test_recover_rank_multiple_nodes(self, deployment_rank_manager):
        """Test recovering replicas across multiple nodes."""
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=0, local_rank=0, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_2", "node_2", global_rank=1, local_rank=0, node_rank=1
        )
        deployment_rank_manager.recover_rank(
            "replica_3", "node_1", global_rank=2, local_rank=1, node_rank=0
        )

        ranks1 = deployment_rank_manager.get_replica_rank("replica_1")
        ranks2 = deployment_rank_manager.get_replica_rank("replica_2")
        ranks3 = deployment_rank_manager.get_replica_rank("replica_3")

        assert ranks1 == (0, 0, 0)
        assert ranks2 == (1, 1, 0)
        assert ranks3 == (2, 0, 1)

    def test_recover_rank_duplicate_replica_fails(self, deployment_rank_manager):
        """Test recovering rank for replica that already has one fails."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")

        with pytest.raises(RuntimeError, match="Rank for .* already assigned"):
            deployment_rank_manager.recover_rank(
                "replica_1", "node_1", global_rank=5, local_rank=0, node_rank=0
            )

    def test_has_replica_rank(self, deployment_rank_manager):
        """Test checking if replica has rank assigned."""
        assert not deployment_rank_manager.has_replica_rank("replica_1")

        deployment_rank_manager.assign_rank("replica_1", "node_1")
        assert deployment_rank_manager.has_replica_rank("replica_1")

        deployment_rank_manager.release_rank("replica_1")
        assert not deployment_rank_manager.has_replica_rank("replica_1")

    def test_get_replica_rank_nonexistent_fails(self, deployment_rank_manager):
        """Test getting rank for non-existent replica fails."""
        with pytest.raises(RuntimeError, match="Rank for .* not assigned"):
            deployment_rank_manager.get_replica_rank("nonexistent")

    def test_clear(self, deployment_rank_manager):
        """Test clearing all rank data."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_2")
        deployment_rank_manager.assign_rank("replica_3", "node_1")

        deployment_rank_manager.clear()

        # Verify all state is cleared
        assert deployment_rank_manager._replica_rank_manager.get_ranks_mapping() == {}
        assert deployment_rank_manager._node_rank_manager.get_ranks_mapping() == {}
        assert deployment_rank_manager._local_rank_managers == {}
        assert deployment_rank_manager._replica_to_node == {}

        # Verify next assigned rank starts from 0 again
        global_rank, node_rank, local_rank = deployment_rank_manager.assign_rank(
            "replica_4", "node_3"
        )
        assert global_rank == 0
        assert node_rank == 0
        assert local_rank == 0


class TestDeploymentRankManagerRankReuse:
    """Test cases for rank reuse after release."""

    def test_global_rank_reuse(self, deployment_rank_manager):
        """Test that released global ranks are reused."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        global_rank2, _, _ = deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_1")

        # Release replica with global rank 1
        deployment_rank_manager.release_rank("replica_2")

        # Next replica should get global rank 1
        global_rank4, _, _ = deployment_rank_manager.assign_rank("replica_4", "node_1")
        assert global_rank4 == global_rank2

    def test_local_rank_reuse(self, deployment_rank_manager):
        """Test that released local ranks are reused."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        _, _, local_rank2 = deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_1")

        # Release replica with local rank 1
        deployment_rank_manager.release_rank("replica_2")

        # Next replica on same node should get local rank 1
        _, _, local_rank4 = deployment_rank_manager.assign_rank("replica_4", "node_1")
        assert local_rank4 == local_rank2

    def test_node_rank_reuse(self, deployment_rank_manager):
        """Test that released node ranks are reused."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        _, node_rank2, _ = deployment_rank_manager.assign_rank("replica_2", "node_2")
        deployment_rank_manager.assign_rank("replica_3", "node_3")

        # Release all replicas on node_2
        deployment_rank_manager.release_rank("replica_2")

        # New node should get the released node rank
        _, node_rank4, _ = deployment_rank_manager.assign_rank("replica_4", "node_4")
        assert node_rank4 == node_rank2


class TestDeploymentRankManagerConsistency:
    """Test cases for rank consistency checks and reassignments."""

    def test_check_consistency_empty_replicas(self, deployment_rank_manager):
        """Test consistency check with no active replicas."""
        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            []
        )
        assert result == []

    def test_check_consistency_all_ranks_contiguous(self, deployment_rank_manager):
        """Test consistency check when all ranks are contiguous."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Set up contiguous ranks across 2 nodes
        deployment_rank_manager.assign_rank(
            "replica_1", "node_1"
        )  # global=0, node=0, local=0
        deployment_rank_manager.assign_rank(
            "replica_2", "node_1"
        )  # global=1, node=0, local=1
        deployment_rank_manager.assign_rank(
            "replica_3", "node_2"
        )  # global=2, node=1, local=0
        deployment_rank_manager.assign_rank(
            "replica_4", "node_2"
        )  # global=3, node=1, local=1

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        assert result == []

    def test_check_consistency_non_contiguous_global_ranks(
        self, deployment_rank_manager
    ):
        """Test consistency check with non-contiguous global ranks."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        # Create gap in global ranks
        deployment_rank_manager.assign_rank("replica_1", "node_1")  # global=0
        deployment_rank_manager.assign_rank("replica_temp", "node_1")  # global=1
        deployment_rank_manager.assign_rank("replica_2", "node_1")  # global=2
        deployment_rank_manager.assign_rank("replica_3", "node_1")  # global=3
        deployment_rank_manager.release_rank("replica_temp")  # Gap at 1

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Some replicas should be reassigned
        assert len(result) > 0

        # After reassignment, global ranks should be [0, 1, 2]
        final_global_ranks = sorted(
            [
                deployment_rank_manager.get_replica_rank("replica_1")[0],
                deployment_rank_manager.get_replica_rank("replica_2")[0],
                deployment_rank_manager.get_replica_rank("replica_3")[0],
            ]
        )
        assert final_global_ranks == [0, 1, 2]

    def test_check_consistency_non_contiguous_local_ranks(
        self, deployment_rank_manager
    ):
        """Test consistency check with non-contiguous local ranks on a node."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        # Create gap in local ranks
        deployment_rank_manager.assign_rank("replica_1", "node_1")  # local=0
        deployment_rank_manager.assign_rank("replica_temp", "node_1")  # local=1
        deployment_rank_manager.assign_rank("replica_2", "node_1")  # local=2
        deployment_rank_manager.assign_rank("replica_3", "node_1")  # local=3
        deployment_rank_manager.release_rank("replica_temp")  # Gap at local=1

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Some replicas should be reassigned
        assert len(result) > 0

        # After reassignment, local ranks should be [0, 1, 2]
        final_local_ranks = sorted(
            [
                deployment_rank_manager.get_replica_rank("replica_1")[2],
                deployment_rank_manager.get_replica_rank("replica_2")[2],
                deployment_rank_manager.get_replica_rank("replica_3")[2],
            ]
        )
        assert final_local_ranks == [0, 1, 2]

    def test_check_consistency_non_contiguous_node_ranks(self, deployment_rank_manager):
        """Test consistency check with non-contiguous node ranks."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        # Create gap in node ranks
        deployment_rank_manager.assign_rank("replica_1", "node_1")  # node_rank=0
        deployment_rank_manager.assign_rank("replica_temp", "node_2")  # node_rank=1
        deployment_rank_manager.assign_rank("replica_2", "node_3")  # node_rank=2
        deployment_rank_manager.assign_rank("replica_3", "node_4")  # node_rank=3
        deployment_rank_manager.release_rank("replica_temp")  # Gap at node_rank=1

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Replicas on reassigned nodes should be marked for reconfiguration
        assert len(result) > 0

        # After reassignment, node ranks should be [0, 1, 2]
        node_ranks = set()
        for replica_id in ["replica_1", "replica_2", "replica_3"]:
            node_id = deployment_rank_manager._replica_to_node[replica_id]
            node_rank = deployment_rank_manager._node_rank_manager.get_rank(node_id)
            node_ranks.add(node_rank)

        assert node_ranks == {0, 1, 2}

    def test_check_consistency_minimal_reassignment(self, deployment_rank_manager):
        """Test that consistency check performs minimal reassignment."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Set up non-contiguous global ranks: 0, 2, 5, 7
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=0, local_rank=0, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_2", "node_1", global_rank=2, local_rank=1, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_3", "node_2", global_rank=5, local_rank=0, node_rank=1
        )
        deployment_rank_manager.recover_rank(
            "replica_4", "node_2", global_rank=7, local_rank=1, node_rank=1
        )

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Should reassign 2 replicas (keep 0 and 2, reassign 5->1 and 7->3)
        assert len(result) == 2
        reassigned_ids = {r.replica_id.unique_id for r in result}
        assert reassigned_ids == {"replica_3", "replica_4"}

        # Verify that replica_1 and replica_2 kept their ranks
        assert deployment_rank_manager.get_replica_rank("replica_1")[0] == 0
        assert deployment_rank_manager.get_replica_rank("replica_2")[0] == 2

    def test_check_consistency_unranked_replicas_fails(self):
        """Test consistency check fails when replicas have no ranks."""
        deployment_rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1]
            )

    def test_check_consistency_stale_ranks_fails(self):
        """Test consistency check fails when there are stale ranks."""
        deployment_rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("stale_replica", "node_1")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1]
            )

    def test_check_consistency_replicas_without_nodes_fails(self):
        """Test consistency check fails when replicas don't have node assignments."""
        deployment_rank_manager = DeploymentRankManager()
        replica1 = MockDeploymentReplica("replica_1")

        # Assign rank but manually remove node assignment (simulates inconsistency)
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager._replica_to_node.pop("replica_1")

        with pytest.raises(AssertionError, match="Replica .* not assigned to any node"):
            deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1]
            )


class TestDeploymentRankManagerEdgeCases:
    """Test edge cases and error conditions."""

    def test_assign_rank_to_replica_on_different_node_fails(
        self, deployment_rank_manager
    ):
        """Test that assigning rank again to replica on different node fails."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")

        with pytest.raises(RuntimeError, match="Rank for .* already assigned"):
            deployment_rank_manager.assign_rank("replica_1", "node_2")

    def test_release_all_replicas_on_node(self, deployment_rank_manager):
        """Test releasing all replicas on a node releases node rank."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_2")

        # Release both replicas on node_1
        deployment_rank_manager.release_rank("replica_1")
        deployment_rank_manager.release_rank("replica_2")

        # Node rank for node_1 should be released
        assert "node_1" not in deployment_rank_manager._local_rank_managers
        assert not deployment_rank_manager._node_rank_manager.has_rank("node_1")

        # Node rank 0 should be reused for new node
        g, n, l = deployment_rank_manager.assign_rank("replica_4", "node_3")
        assert n == 0  # Node rank was reused

    def test_interleaved_assign_release_recover(self, deployment_rank_manager):
        """Test complex interleaved operations."""
        # Assign ranks
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_2")

        # Release one replica
        deployment_rank_manager.release_rank("replica_2")

        # Recover a different replica with specific ranks
        deployment_rank_manager.recover_rank("replica_4", "node_3", 10, 5, 2)

        # Assign new replica (should reuse global rank 1)
        g, n, l = deployment_rank_manager.assign_rank("replica_5", "node_1")
        assert g == 1  # Reused rank

        # Verify final ranks using get_replica_rank
        assert deployment_rank_manager.get_replica_rank("replica_1") == (0, 0, 0)
        assert deployment_rank_manager.get_replica_rank("replica_5") == (1, 0, 1)
        assert deployment_rank_manager.get_replica_rank("replica_3") == (2, 1, 0)
        assert deployment_rank_manager.get_replica_rank("replica_4") == (10, 2, 5)

    def test_large_scale_operations(self, deployment_rank_manager):
        """Test operations with many replicas and nodes."""
        num_nodes = 10
        replicas_per_node = 5

        # Assign many replicas across many nodes
        replica_count = 0
        for node_idx in range(num_nodes):
            node_id = f"node_{node_idx}"
            for local_idx in range(replicas_per_node):
                replica_id = f"replica_{replica_count}"
                g, n, l = deployment_rank_manager.assign_rank(replica_id, node_id)

                assert g == replica_count
                assert n == node_idx
                assert l == local_idx

                replica_count += 1

        # Verify internal state
        assert len(deployment_rank_manager._local_rank_managers) == num_nodes
        assert (
            len(deployment_rank_manager._replica_to_node)
            == num_nodes * replicas_per_node
        )

        # Release all replicas from one node
        node_to_remove = "node_5"
        for local_idx in range(replicas_per_node):
            replica_id = f"replica_{5 * replicas_per_node + local_idx}"
            deployment_rank_manager.release_rank(replica_id)

        # Verify node was removed
        assert node_to_remove not in deployment_rank_manager._local_rank_managers
        assert not deployment_rank_manager._node_rank_manager.has_rank(node_to_remove)
        assert len(deployment_rank_manager._local_rank_managers) == num_nodes - 1

    def test_get_replica_rank_tuple(self, deployment_rank_manager):
        """Test that get_replica_rank returns correct tuple of all three ranks."""
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_2")

        # Verify tuple format: (global_rank, node_rank, local_rank)
        assert deployment_rank_manager.get_replica_rank("replica_1") == (0, 0, 0)
        assert deployment_rank_manager.get_replica_rank("replica_2") == (1, 0, 1)
        assert deployment_rank_manager.get_replica_rank("replica_3") == (2, 1, 0)

    def test_get_replica_rank_fails_when_not_assigned(self, deployment_rank_manager):
        """Test that get_replica_rank fails for non-existent replica."""
        with pytest.raises(RuntimeError, match="Rank for .* not assigned"):
            deployment_rank_manager.get_replica_rank("nonexistent")

    def test_internal_state_consistency(self, deployment_rank_manager):
        """Test that internal state structures remain consistent."""
        # Assign replicas
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_2")
        deployment_rank_manager.assign_rank("replica_3", "node_1")

        # Verify internal consistency
        assert len(deployment_rank_manager._replica_to_node) == 3
        assert len(deployment_rank_manager._local_rank_managers) == 2
        assert deployment_rank_manager._node_rank_manager.has_rank("node_1")
        assert deployment_rank_manager._node_rank_manager.has_rank("node_2")

        # Release one replica
        deployment_rank_manager.release_rank("replica_2")

        # Verify consistency after release
        assert len(deployment_rank_manager._replica_to_node) == 2
        assert len(deployment_rank_manager._local_rank_managers) == 1  # node_2 removed
        assert not deployment_rank_manager._node_rank_manager.has_rank("node_2")


class TestDeploymentRankManagerComplexScenarios:
    """Test cases for complex real-world scenarios."""

    def test_scale_up_on_existing_nodes(self, deployment_rank_manager):
        """Test scaling up by adding replicas to existing nodes."""
        # Initial state: 2 replicas on 2 nodes
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_2")

        # Scale up: add 2 more replicas
        global_rank3, node_rank3, local_rank3 = deployment_rank_manager.assign_rank(
            "replica_3", "node_1"
        )
        global_rank4, node_rank4, local_rank4 = deployment_rank_manager.assign_rank(
            "replica_4", "node_2"
        )

        # Global ranks continue from 2
        assert global_rank3 == 2
        assert global_rank4 == 3

        # Node ranks stay the same (existing nodes)
        assert node_rank3 == 0
        assert node_rank4 == 1

        # Local ranks increment on each node
        assert local_rank3 == 1
        assert local_rank4 == 1

    def test_scale_down(self, deployment_rank_manager):
        """Test scaling down by removing replicas."""
        # Initial state: 4 replicas on 2 nodes
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_2")
        deployment_rank_manager.assign_rank("replica_4", "node_2")

        # Scale down: remove 2 replicas
        deployment_rank_manager.release_rank("replica_2")
        deployment_rank_manager.release_rank("replica_4")

        # Remaining replicas should still have correct ranks
        assert deployment_rank_manager.get_replica_rank("replica_1") == (0, 0, 0)
        assert deployment_rank_manager.get_replica_rank("replica_3") == (2, 1, 0)

    def test_rolling_update_scenario(self, deployment_rank_manager):
        """Test a rolling update scenario where replicas are replaced."""
        # Initial state: 3 replicas
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_2")
        deployment_rank_manager.assign_rank("replica_3", "node_3")

        # Replace replica_2 (stop old, start new)
        deployment_rank_manager.release_rank("replica_2")
        global_rank, node_rank, local_rank = deployment_rank_manager.assign_rank(
            "replica_2_new", "node_2"
        )

        # New replica should reuse the released global rank
        assert global_rank == 1
        # Node rank should be reused since node_2 still exists
        assert node_rank == 1
        # Local rank should be 0 (first on node_2 again)
        assert local_rank == 0

    def test_node_failure_recovery(self, deployment_rank_manager):
        """Test recovering from a node failure."""
        # Initial state: 4 replicas on 2 nodes
        deployment_rank_manager.assign_rank("replica_1", "node_1")
        deployment_rank_manager.assign_rank("replica_2", "node_1")
        deployment_rank_manager.assign_rank("replica_3", "node_2")
        deployment_rank_manager.assign_rank("replica_4", "node_2")

        # Node 2 fails, remove all its replicas
        deployment_rank_manager.release_rank("replica_3")
        deployment_rank_manager.release_rank("replica_4")

        # Node 2's node rank should be released
        assert not deployment_rank_manager._node_rank_manager.has_rank("node_2")

        # Recovery: start new replicas on node_3
        global_rank3, node_rank3, local_rank3 = deployment_rank_manager.assign_rank(
            "replica_5", "node_3"
        )
        global_rank4, node_rank4, local_rank4 = deployment_rank_manager.assign_rank(
            "replica_6", "node_3"
        )

        # Should reuse released global ranks
        assert global_rank3 == 2
        assert global_rank4 == 3
        # Should reuse released node rank
        assert node_rank3 == 1
        assert node_rank4 == 1
        # Local ranks start from 0 on new node
        assert local_rank3 == 0
        assert local_rank4 == 1

    def test_controller_restart_recovery(self, deployment_rank_manager):
        """Test recovering state after controller restart."""
        # Simulate replicas that were running before controller restart
        # Recover them with their previous ranks
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=0, local_rank=0, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_2", "node_1", global_rank=1, local_rank=1, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_3", "node_2", global_rank=2, local_rank=0, node_rank=1
        )

        # Verify all ranks are recovered correctly
        assert deployment_rank_manager.get_replica_rank("replica_1") == (0, 0, 0)
        assert deployment_rank_manager.get_replica_rank("replica_2") == (1, 0, 1)
        assert deployment_rank_manager.get_replica_rank("replica_3") == (2, 1, 0)

        # New replica should get rank 3
        global_rank4, _, _ = deployment_rank_manager.assign_rank("replica_4", "node_1")
        assert global_rank4 == 3

    def test_multi_node_consistency_check(self, deployment_rank_manager):
        """Test consistency check across multiple nodes with gaps in all rank types."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Create gaps in all three rank systems
        deployment_rank_manager.recover_rank(
            "replica_1", "node_1", global_rank=0, local_rank=0, node_rank=0
        )
        deployment_rank_manager.recover_rank(
            "replica_2",
            "node_3",  # gap in node rank
            global_rank=3,
            local_rank=0,
            node_rank=2,
        )
        deployment_rank_manager.recover_rank(
            "replica_3", "node_1", global_rank=5, local_rank=2, node_rank=0
        )  # gap in local rank
        deployment_rank_manager.recover_rank(
            "replica_4",
            "node_4",  # gap in node rank
            global_rank=7,
            local_rank=0,
            node_rank=4,
        )

        result = deployment_rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Should reassign replicas to fix all gaps
        assert len(result) > 0

        # Verify all ranks are now contiguous
        global_ranks = sorted(
            [
                deployment_rank_manager.get_replica_rank(f"replica_{i}")[0]
                for i in range(1, 5)
            ]
        )
        assert global_ranks == [0, 1, 2, 3]

        # Verify node ranks are contiguous
        node_ranks = set()
        for i in range(1, 5):
            node_id = deployment_rank_manager._replica_to_node[f"replica_{i}"]
            node_rank = deployment_rank_manager._node_rank_manager.get_rank(node_id)
            node_ranks.add(node_rank)
        assert node_ranks == {0, 1, 2}  # 3 unique nodes

        # Verify local ranks are contiguous per node
        for node_id in deployment_rank_manager._local_rank_managers:
            local_ranks = sorted(
                deployment_rank_manager._local_rank_managers[node_id]
                .get_ranks_mapping()
                .values()
            )
            expected_local = list(range(len(local_ranks)))
            assert local_ranks == expected_local


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
