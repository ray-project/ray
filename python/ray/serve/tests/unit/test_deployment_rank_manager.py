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

    def test_assign_rank_first_replica(self, rank_manager: DeploymentRankManager):
        """Test assigning rank to first replica."""
        rank = rank_manager.assign_rank("replica_1", "node_1")
        assert rank.rank == 0
        assert rank_manager.has_replica_rank("replica_1")
        assert rank_manager.get_replica_rank("replica_1").rank == 0

    def test_assign_rank_multiple_replicas(self, rank_manager: DeploymentRankManager):
        """Test assigning ranks to multiple replicas."""
        rank1 = rank_manager.assign_rank("replica_1", "node_1")
        rank2 = rank_manager.assign_rank("replica_2", "node_1")
        rank3 = rank_manager.assign_rank("replica_3", "node_1")

        assert rank1.rank == 0
        assert rank2.rank == 1
        assert rank3.rank == 2

        mapping = rank_manager.get_replica_ranks_mapping()
        assert len(mapping) == 3
        assert mapping == {
            "replica_1": ReplicaRank(rank=0, node_rank=0, local_rank=0),
            "replica_2": ReplicaRank(rank=1, node_rank=0, local_rank=1),
            "replica_3": ReplicaRank(rank=2, node_rank=0, local_rank=2),
        }

    def test_assign_rank_reuses_released_ranks(
        self, rank_manager: DeploymentRankManager
    ):
        """Test that released ranks are reused before assigning new ones."""
        # Assign ranks to 3 replicas
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("replica_2", "node_1")
        rank_manager.assign_rank("replica_3", "node_1")

        # Release middle rank
        rank_manager.release_rank("replica_2")
        assert not rank_manager.has_replica_rank("replica_2")

        # New replica should get the released rank (1)
        rank = rank_manager.assign_rank("replica_4", "node_1")
        assert rank.rank == 1
        assert rank_manager.get_replica_rank("replica_4") == ReplicaRank(
            rank=1, node_rank=0, local_rank=1
        )

    def test_assign_rank_duplicate_fails(self, rank_manager: DeploymentRankManager):
        """Test assigning rank to replica that already has one fails."""
        rank_manager.assign_rank("replica_1", "node_1")

        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.assign_rank("replica_1", "node_1")

    def test_release_rank(self, rank_manager: DeploymentRankManager):
        """Test releasing a rank makes it available for reuse."""
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("replica_2", "node_1")

        rank_manager.release_rank("replica_1")

        assert not rank_manager.has_replica_rank("replica_1")
        assert rank_manager.has_replica_rank("replica_2")
        assert rank_manager.get_replica_rank("replica_2").rank == 1

    def test_release_rank_nonexistent_replica(
        self, rank_manager: DeploymentRankManager
    ):
        """Test releasing rank for non-existent replica fails."""
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.release_rank("nonexistent")

    def test_recover_rank_basic(self, rank_manager: DeploymentRankManager):
        """Test basic rank recovery."""
        rank_manager.recover_rank(
            "replica_1", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
        )

        assert rank_manager.has_replica_rank("replica_1")
        assert rank_manager.get_replica_rank("replica_1").rank == 5

    def test_recover_rank_updates_next_rank(self, rank_manager: DeploymentRankManager):
        """Test that recovering a high rank updates next_rank appropriately."""
        rank_manager.assign_rank("replica_1", "node_1")  # Gets rank 0
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=10, node_rank=0, local_rank=0)
        )

        # New replica should get rank 11 (next available after 10)
        rank = rank_manager.assign_rank("replica_3", "node_1")
        assert rank.rank == 11

        mapping = rank_manager.get_replica_ranks_mapping()
        assert mapping == {
            "replica_1": ReplicaRank(rank=0, node_rank=0, local_rank=0),
            "replica_2": ReplicaRank(rank=10, node_rank=0, local_rank=0),
            "replica_3": ReplicaRank(rank=11, node_rank=0, local_rank=1),
        }

    def test_recover_rank_removes_from_available(
        self, rank_manager: DeploymentRankManager
    ):
        """Test that recovering a rank removes it from available ranks."""
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("replica_2", "node_1")
        rank_manager.release_rank("replica_1")  # Rank 0 becomes available

        # Recover rank 0 for a new replica
        rank_manager.recover_rank(
            "replica_3", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

        # Verify replica_3 has rank 0
        assert rank_manager.has_replica_rank("replica_3")
        assert rank_manager.get_replica_rank("replica_3").rank == 0

        # Next assigned replica should get rank 2 (not 0, which is now taken)
        rank = rank_manager.assign_rank("replica_4", "node_1")
        assert rank.rank == 2

    def test_recover_rank_duplicate_fails(self, rank_manager: DeploymentRankManager):
        """Test recovering rank for replica that already has one fails."""
        rank_manager.assign_rank("replica_1", "node_1")

        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.recover_rank(
                "replica_1", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
            )

    def test_get_replica_rank_existing(self, rank_manager: DeploymentRankManager):
        """Test getting rank for existing replica."""
        rank_manager.assign_rank("replica_1", "node_1")
        rank = rank_manager.get_replica_rank("replica_1")
        assert rank.rank == 0

    def test_get_replica_rank_nonexistent_fails(
        self, rank_manager: DeploymentRankManager
    ):
        """Test getting rank for non-existent replica fails."""
        with pytest.raises(RuntimeError, match="not assigned"):
            rank_manager.get_replica_rank("nonexistent")

    def test_get_replica_ranks_mapping(self, rank_manager: DeploymentRankManager):
        """Test getting copy of replica ranks mapping."""
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("replica_2", "node_1")

        mapping = rank_manager.get_replica_ranks_mapping()
        expected = {
            "replica_1": ReplicaRank(rank=0, node_rank=0, local_rank=0),
            "replica_2": ReplicaRank(rank=1, node_rank=0, local_rank=1),
        }

        assert mapping == expected

        # Verify it's a copy by modifying it
        mapping["replica_3"] = ReplicaRank(rank=2, node_rank=0, local_rank=2)
        # Get a fresh mapping to verify the original wasn't changed
        fresh_mapping = rank_manager.get_replica_ranks_mapping()
        assert "replica_3" not in fresh_mapping
        assert fresh_mapping == expected

    def test_clear(self, rank_manager: DeploymentRankManager):
        """Test clearing all rank data."""
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("replica_2", "node_1")
        rank_manager.release_rank("replica_1")

        rank_manager.clear()

        # After clearing, should have no ranks
        assert rank_manager.get_replica_ranks_mapping() == {}
        assert not rank_manager.has_replica_rank("replica_1")
        assert not rank_manager.has_replica_rank("replica_2")

        # Should be able to assign from 0 again
        rank = rank_manager.assign_rank("replica_3", "node_1")
        assert rank.rank == 0

    def test_check_rank_consistency_empty_replicas(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check with no active replicas."""
        result = rank_manager.check_rank_consistency_and_reassign_minimally([])
        assert result == []

    def test_check_rank_consistency_contiguous_ranks(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check with contiguous ranks (no reassignment needed)."""
        # Set up contiguous ranks
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        rank_manager.assign_rank("replica_1", "node_1")  # rank 0
        rank_manager.assign_rank("replica_2", "node_1")  # rank 1
        rank_manager.assign_rank("replica_3", "node_1")  # rank 2

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        assert result == []

    def test_check_rank_consistency_non_contiguous_ranks(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check with non-contiguous ranks (reassignment needed)."""
        # Set up non-contiguous ranks (simulate a replica being removed)
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")

        # Manually assign non-contiguous ranks using recover_rank
        rank_manager.recover_rank(
            "replica_1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=2, node_rank=0, local_rank=1)
        )  # Gap at rank 1
        rank_manager.recover_rank(
            "replica_3", "node_1", ReplicaRank(rank=3, node_rank=0, local_rank=2)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Should reassign some replicas to make ranks contiguous
        assert len(result) > 0

        # After reassignment, ranks should be contiguous [0, 1, 2]
        mapping = rank_manager.get_replica_ranks_mapping()
        final_ranks = sorted([r.rank for r in mapping.values()])
        expected_ranks = [0, 1, 2]
        assert final_ranks == expected_ranks

    def test_minimal_reassignment_keeps_existing_when_possible(
        self, rank_manager: DeploymentRankManager
    ):
        """Test that minimal reassignment keeps existing ranks when possible."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        replica4 = MockDeploymentReplica("replica_4")

        # Set up ranks: 0, 2, 5, 7 (non-contiguous) using recover_rank
        rank_manager.recover_rank(
            "replica_1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )  # Should keep this
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=2, node_rank=0, local_rank=1)
        )  # Should keep this
        rank_manager.recover_rank(
            "replica_3", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=2)
        )  # Should be reassigned to 1
        rank_manager.recover_rank(
            "replica_4", "node_1", ReplicaRank(rank=7, node_rank=0, local_rank=3)
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
        final_ranks = sorted([r.rank for r in mapping.values()])
        assert final_ranks == [0, 1, 2, 3]

        # Verify that replica_1 and replica_2 kept their original ranks
        assert rank_manager.get_replica_rank("replica_1").rank == 0
        assert rank_manager.get_replica_rank("replica_2").rank == 2

    def test_check_rank_consistency_unranked_replicas_fails(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check fails when active replicas have no ranks."""
        replica1 = MockDeploymentReplica("replica_1")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_stale_ranks_fails(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check fails when there are stale ranks."""
        replica1 = MockDeploymentReplica("replica_1")

        # Set up stale rank (replica not in active list)
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("stale_replica", "node_1")

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally([replica1])

    def test_check_rank_consistency_duplicate_ranks_fails(
        self, rank_manager: DeploymentRankManager
    ):
        """Test consistency check fails when there are duplicate ranks."""
        replica1 = MockDeploymentReplica("replica_1")
        replica2 = MockDeploymentReplica("replica_2")

        # Manually create duplicate ranks using recover_rank (this should never happen in normal operation)
        # Note: We can only test this with duplicate global ranks, not duplicate local ranks
        # since local_rank uniqueness is enforced by the underlying RankManager
        rank_manager.recover_rank(
            "replica_1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=1)
        )  # Duplicate global rank!

        with pytest.raises(RuntimeError, match="Rank system is in an invalid state"):
            rank_manager.check_rank_consistency_and_reassign_minimally(
                [replica1, replica2]
            )


class TestDeploymentRankManagerMultiNode:
    def test_assign_rank_multiple_nodes(self):
        """Test that replicas on different nodes get correct node_rank and local_rank."""
        rank_manager = DeploymentRankManager()

        # Assign replicas across different nodes
        rank1 = rank_manager.assign_rank("replica_1", "node_1")
        rank2 = rank_manager.assign_rank("replica_2", "node_2")
        rank3 = rank_manager.assign_rank("replica_3", "node_1")
        rank4 = rank_manager.assign_rank("replica_4", "node_2")
        rank5 = rank_manager.assign_rank("replica_5", "node_3")

        # Verify global ranks are sequential
        assert rank1.rank == 0
        assert rank2.rank == 1
        assert rank3.rank == 2
        assert rank4.rank == 3
        assert rank5.rank == 4

        # Verify node ranks are assigned correctly
        assert rank1.node_rank == 0  # node_1 is first
        assert rank2.node_rank == 1  # node_2 is second
        assert rank3.node_rank == 0  # node_1 already has rank 0
        assert rank4.node_rank == 1  # node_2 already has rank 1
        assert rank5.node_rank == 2  # node_3 is third

        # Verify local ranks restart per node
        assert rank1.local_rank == 0  # First replica on node_1
        assert rank2.local_rank == 0  # First replica on node_2
        assert rank3.local_rank == 1  # Second replica on node_1
        assert rank4.local_rank == 1  # Second replica on node_2
        assert rank5.local_rank == 0  # First replica on node_3

    def test_local_rank_independence_across_nodes(self):
        """Test that local_rank sequences are independent per node."""
        rank_manager = DeploymentRankManager()

        # Add 3 replicas to node_1
        r1 = rank_manager.assign_rank("n1_r1", "node_1")
        r2 = rank_manager.assign_rank("n1_r2", "node_1")
        r3 = rank_manager.assign_rank("n1_r3", "node_1")

        # Add 2 replicas to node_2
        r4 = rank_manager.assign_rank("n2_r1", "node_2")
        r5 = rank_manager.assign_rank("n2_r2", "node_2")

        # Verify local ranks are independent
        assert r1.local_rank == 0
        assert r2.local_rank == 1
        assert r3.local_rank == 2
        assert r4.local_rank == 0  # Restarts at 0 for node_2
        assert r5.local_rank == 1

    def test_release_rank_removes_node_when_last_replica(self):
        """Test that releasing the last replica on a node releases the node rank."""
        rank_manager = DeploymentRankManager()

        # Assign replicas to two nodes
        rank_manager.assign_rank("n1_r1", "node_1")
        rank_manager.assign_rank("n2_r1", "node_2")
        rank_manager.assign_rank("n2_r2", "node_2")

        # Release the only replica on node_1
        rank_manager.release_rank("n1_r1")

        # Now add a replica to node_3 - it should get node_rank 0 (reused from node_1)
        new_rank = rank_manager.assign_rank("n3_r1", "node_3")
        assert new_rank.node_rank == 0  # Reused node rank

    def test_release_rank_keeps_node_when_replicas_remain(self):
        """Test that releasing a replica doesn't release node rank if other replicas remain."""
        rank_manager = DeploymentRankManager()

        # Assign 3 replicas to node_1
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.assign_rank("r2", "node_1")
        rank_manager.assign_rank("r3", "node_1")

        # Assign replica to node_2
        node2_rank = rank_manager.assign_rank("r4", "node_2")
        assert node2_rank.node_rank == 1

        # Release one replica from node_1
        rank_manager.release_rank("r2")

        # Add another replica to node_1 - should still have node_rank 0
        new_rank = rank_manager.assign_rank("r5", "node_1")
        assert new_rank.node_rank == 0  # Node rank preserved

    def test_node_rank_reuse_after_complete_release(self):
        """Test that node ranks are reused after all replicas are removed."""
        rank_manager = DeploymentRankManager()

        # Create replicas on 3 nodes
        rank_manager.assign_rank("n1_r1", "node_1")
        rank_manager.assign_rank("n2_r1", "node_2")
        rank_manager.assign_rank("n3_r1", "node_3")

        # Verify node ranks
        assert rank_manager.get_replica_rank("n1_r1").node_rank == 0
        assert rank_manager.get_replica_rank("n2_r1").node_rank == 1
        assert rank_manager.get_replica_rank("n3_r1").node_rank == 2

        # Remove all replicas from node_2 (middle node)
        rank_manager.release_rank("n2_r1")

        # Add replica to a new node - should reuse node_rank 1
        new_rank = rank_manager.assign_rank("n4_r1", "node_4")
        assert new_rank.node_rank == 1  # Reused from released node_2

    def test_local_rank_reuse_within_node(self):
        """Test that local ranks are reused within a node after release."""
        rank_manager = DeploymentRankManager()

        # Assign 3 replicas to node_1
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.assign_rank("r2", "node_1")
        rank_manager.assign_rank("r3", "node_1")

        # Release middle replica (local_rank=1)
        rank_manager.release_rank("r2")

        # Add new replica to node_1 - should reuse local_rank 1
        new_rank = rank_manager.assign_rank("r4", "node_1")
        assert new_rank.local_rank == 1  # Reused local rank

    def test_recover_rank_multiple_nodes(self):
        """Test recovering ranks for replicas on different nodes."""
        rank_manager = DeploymentRankManager()

        # Recover replicas on different nodes
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_2", ReplicaRank(rank=1, node_rank=1, local_rank=0)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=2, node_rank=0, local_rank=1)
        )

        # Verify all ranks are recovered correctly
        assert rank_manager.get_replica_rank("r1") == ReplicaRank(
            rank=0, node_rank=0, local_rank=0
        )
        assert rank_manager.get_replica_rank("r2") == ReplicaRank(
            rank=1, node_rank=1, local_rank=0
        )
        assert rank_manager.get_replica_rank("r3") == ReplicaRank(
            rank=2, node_rank=0, local_rank=1
        )

    def test_recover_rank_preserves_node_rank_when_node_exists(self):
        """Test that recovering a replica on an existing node doesn't create duplicate node ranks."""
        rank_manager = DeploymentRankManager()

        # Recover first replica on node_1
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

        # Recover another replica on the same node
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=1, node_rank=0, local_rank=1)
        )

        # Both should have the same node_rank
        assert rank_manager.get_replica_rank("r1").node_rank == 0
        assert rank_manager.get_replica_rank("r2").node_rank == 0

    def test_check_rank_consistency_across_multiple_nodes(self):
        """Test consistency checking with replicas spread across nodes."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")
        replica4 = MockDeploymentReplica("r4")

        # Set up non-contiguous global ranks across nodes
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_2", ReplicaRank(rank=5, node_rank=1, local_rank=0)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=8, node_rank=0, local_rank=1)
        )
        rank_manager.recover_rank(
            "r4", "node_2", ReplicaRank(rank=10, node_rank=1, local_rank=1)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Should reassign some replicas to make global ranks contiguous
        assert len(result) > 0

        # Verify global ranks are now contiguous
        mapping = rank_manager.get_replica_ranks_mapping()
        global_ranks = sorted([r.rank for r in mapping.values()])
        assert global_ranks == [0, 1, 2, 3]

    def test_check_rank_consistency_local_ranks_per_node(self):
        """Test that local rank consistency is checked independently per node."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")
        replica4 = MockDeploymentReplica("r4")

        # Set up non-contiguous local ranks on node_1, contiguous on node_2
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=1, node_rank=0, local_rank=5)
        )  # Gap in local rank
        rank_manager.recover_rank(
            "r3", "node_2", ReplicaRank(rank=2, node_rank=1, local_rank=0)
        )
        rank_manager.recover_rank(
            "r4", "node_2", ReplicaRank(rank=3, node_rank=1, local_rank=1)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Should reassign replica on node_1 with non-contiguous local rank
        assert len(result) > 0

        # Verify local ranks are contiguous per node
        r1_rank = rank_manager.get_replica_rank("r1")
        r2_rank = rank_manager.get_replica_rank("r2")
        r3_rank = rank_manager.get_replica_rank("r3")
        r4_rank = rank_manager.get_replica_rank("r4")

        # Node 1 local ranks should be [0, 1]
        node1_local_ranks = sorted([r1_rank.local_rank, r2_rank.local_rank])
        assert node1_local_ranks == [0, 1]

        # Node 2 local ranks should be [0, 1]
        node2_local_ranks = sorted([r3_rank.local_rank, r4_rank.local_rank])
        assert node2_local_ranks == [0, 1]

    def test_check_rank_consistency_node_ranks(self):
        """Test that node rank consistency is maintained."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")

        # Set up non-contiguous node ranks (0, 2, 5)
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_2", ReplicaRank(rank=1, node_rank=2, local_rank=0)
        )
        rank_manager.recover_rank(
            "r3", "node_3", ReplicaRank(rank=2, node_rank=5, local_rank=0)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Should reassign replicas to make node ranks contiguous
        assert len(result) > 0

        # Verify node ranks are now contiguous [0, 1, 2]
        mapping = rank_manager.get_replica_ranks_mapping()
        node_ranks = sorted([r.node_rank for r in mapping.values()])
        assert node_ranks == [0, 1, 2]

    def test_clear_with_multiple_nodes(self):
        """Test that clear() removes all node-related state."""
        rank_manager = DeploymentRankManager()

        # Assign replicas to multiple nodes
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.assign_rank("r2", "node_2")
        rank_manager.assign_rank("r3", "node_3")

        # Clear everything
        rank_manager.clear()

        # Verify all state is cleared
        assert rank_manager.get_replica_ranks_mapping() == {}

        # Verify we can assign fresh ranks starting from 0
        new_rank = rank_manager.assign_rank("r4", "node_4")
        assert new_rank.rank == 0
        assert new_rank.node_rank == 0
        assert new_rank.local_rank == 0

    def test_get_replica_ranks_mapping_multiple_nodes(self):
        """Test that get_replica_ranks_mapping includes correct values for all nodes."""
        rank_manager = DeploymentRankManager()

        # Create replicas across multiple nodes
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.assign_rank("r2", "node_2")
        rank_manager.assign_rank("r3", "node_1")
        rank_manager.assign_rank("r4", "node_3")

        mapping = rank_manager.get_replica_ranks_mapping()

        # Verify all replicas are in mapping with correct values
        assert len(mapping) == 4
        assert mapping["r1"] == ReplicaRank(rank=0, node_rank=0, local_rank=0)
        assert mapping["r2"] == ReplicaRank(rank=1, node_rank=1, local_rank=0)
        assert mapping["r3"] == ReplicaRank(rank=2, node_rank=0, local_rank=1)
        assert mapping["r4"] == ReplicaRank(rank=3, node_rank=2, local_rank=0)

    def test_complex_multi_node_lifecycle(self):
        """Test a complex scenario with adds, releases, and consistency checks across nodes."""
        rank_manager = DeploymentRankManager()

        # Phase 1: Initial deployment across 3 nodes
        rank_manager.assign_rank("n1_r1", "node_1")
        rank_manager.assign_rank("n1_r2", "node_1")
        rank_manager.assign_rank("n2_r1", "node_2")
        rank_manager.assign_rank("n3_r1", "node_3")
        rank_manager.assign_rank("n3_r2", "node_3")

        # Phase 2: Scale down - remove some replicas
        rank_manager.release_rank("n1_r2")  # Remove from node_1
        rank_manager.release_rank("n2_r1")  # Remove all from node_2

        # Phase 3: Scale up - add replicas to new and existing nodes
        rank_manager.assign_rank("n1_r3", "node_1")  # Add to existing node_1
        rank_manager.assign_rank("n4_r1", "node_4")  # New node

        # Verify state is consistent
        mapping = rank_manager.get_replica_ranks_mapping()
        assert len(mapping) == 5

        # Verify node ranks - node_2 was removed, so node_4 should reuse its rank
        assert mapping["n4_r1"].node_rank == 1  # Reused from node_2

        # Verify local ranks per node
        assert mapping["n1_r1"].local_rank == 0
        assert mapping["n1_r3"].local_rank == 1  # Reused local rank
        assert mapping["n3_r1"].local_rank == 0
        assert mapping["n3_r2"].local_rank == 1
        assert mapping["n4_r1"].local_rank == 0

    def test_scaling_up_and_down_across_nodes(self):
        """Test scaling operations across multiple nodes."""
        rank_manager = DeploymentRankManager()

        # Scale up: Add replicas to 4 nodes
        for node_idx in range(4):
            for replica_idx in range(2):
                replica_id = f"n{node_idx}_r{replica_idx}"
                rank_manager.assign_rank(replica_id, f"node_{node_idx}")

        # Verify 8 replicas total
        assert len(rank_manager.get_replica_ranks_mapping()) == 8

        # Scale down: Remove all replicas from nodes 1 and 3
        rank_manager.release_rank("n1_r0")
        rank_manager.release_rank("n1_r1")
        rank_manager.release_rank("n3_r0")
        rank_manager.release_rank("n3_r1")

        # Verify 4 replicas remain
        assert len(rank_manager.get_replica_ranks_mapping()) == 4

        # Scale up again: Add to new node
        new_rank = rank_manager.assign_rank("n5_r0", "node_5")

        # Should reuse a released node rank (1 or 3)
        assert new_rank.node_rank in [1, 3]

    def test_minimal_reassignment_preserves_node_assignments(self):
        """Test that minimal reassignment doesn't move replicas between nodes."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")
        replica4 = MockDeploymentReplica("r4")

        # Set up non-contiguous ranks across nodes
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_2", ReplicaRank(rank=5, node_rank=1, local_rank=0)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=10, node_rank=0, local_rank=1)
        )
        rank_manager.recover_rank(
            "r4", "node_2", ReplicaRank(rank=15, node_rank=1, local_rank=1)
        )

        # Perform consistency check
        rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3, replica4]
        )

        # Verify replicas stayed on their original nodes
        mapping = rank_manager.get_replica_ranks_mapping()

        # r1 and r3 should still be on node_1 (node_rank=0)
        assert mapping["r1"].node_rank == mapping["r3"].node_rank == 0

        # r2 and r4 should still be on node_2 (node_rank=1)
        assert mapping["r2"].node_rank == mapping["r4"].node_rank == 1

        # Verify local ranks are contiguous per node
        assert sorted([mapping["r1"].local_rank, mapping["r3"].local_rank]) == [0, 1]
        assert sorted([mapping["r2"].local_rank, mapping["r4"].local_rank]) == [0, 1]


class TestDeploymentRankManagerEdgeCases:
    """Test edge cases and corner cases for correctness."""

    def test_recover_rank_updates_next_rank_correctly(self):
        """When recovering a rank >= _next_rank, verify next assigned rank is correct."""
        rank_manager = DeploymentRankManager()

        # Assign first replica normally - gets rank 0
        rank_manager.assign_rank("r1", "node_1")

        # Recover a replica with a very high rank
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=100, node_rank=0, local_rank=1)
        )

        # Now assign a new replica - should get rank 101 (next after 100)
        new_rank = rank_manager.assign_rank("r3", "node_1")
        assert new_rank.rank == 101

    def test_assign_after_recovery_with_very_high_rank(self):
        """Test that assignment after recovering a very high rank works correctly."""
        rank_manager = DeploymentRankManager()

        # Recover with rank 1000
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=1000, node_rank=0, local_rank=0)
        )

        # Assign new replica - should get 1001
        new_rank = rank_manager.assign_rank("r2", "node_1")
        assert new_rank.rank == 1001
        assert new_rank.local_rank == 1  # Second replica on node_1

    def test_recover_multiple_high_ranks_out_of_order(self):
        """Test recovering multiple high ranks in non-sequential order."""
        rank_manager = DeploymentRankManager()

        # Recover ranks in random order: 50, 10, 100
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=50, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_2", ReplicaRank(rank=10, node_rank=1, local_rank=0)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=100, node_rank=0, local_rank=1)
        )

        # Next assignment should get 101 (max + 1)
        new_rank = rank_manager.assign_rank("r4", "node_3")
        assert new_rank.rank == 101

    def test_recover_rank_removes_from_released_ranks(self):
        """Test that recovering a rank that was released removes it from available set."""
        rank_manager = DeploymentRankManager()

        # Assign and then release rank 5
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=1)
        )
        rank_manager.release_rank("r2")  # Rank 5 is now released

        # Recover a different replica with rank 5
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=1)
        )

        # Assign new replica - should get rank 6, not 5 (since 5 is taken)
        new_rank = rank_manager.assign_rank("r4", "node_1")
        assert new_rank.rank == 6

    def test_reassignment_with_all_ranks_out_of_range(self):
        """Test reassignment when all replicas have ranks outside target range."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")

        # All replicas have ranks outside [0, 1, 2] range
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=10, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=20, node_rank=0, local_rank=1)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=30, node_rank=0, local_rank=2)
        )

        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # All replicas should be reassigned
        assert len(result) == 3

        # Verify final ranks are exactly [0, 1, 2]
        mapping = rank_manager.get_replica_ranks_mapping()
        final_ranks = sorted([r.rank for r in mapping.values()])
        assert final_ranks == [0, 1, 2]

    def test_reassignment_preserves_target_ranks_exactly(self):
        """Test that after reassignment, ranks are exactly [0, N-1]."""
        rank_manager = DeploymentRankManager()

        replicas = []
        for i in range(5):
            replicas.append(MockDeploymentReplica(f"r{i}"))

        # Create non-contiguous ranks: 0, 3, 7, 11, 15
        rank_manager.recover_rank(
            "r0", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=3, node_rank=0, local_rank=1)
        )
        rank_manager.recover_rank(
            "r2", "node_1", ReplicaRank(rank=7, node_rank=0, local_rank=2)
        )
        rank_manager.recover_rank(
            "r3", "node_1", ReplicaRank(rank=11, node_rank=0, local_rank=3)
        )
        rank_manager.recover_rank(
            "r4", "node_1", ReplicaRank(rank=15, node_rank=0, local_rank=4)
        )

        rank_manager.check_rank_consistency_and_reassign_minimally(replicas)

        # Verify ranks are exactly [0, 1, 2, 3, 4]
        mapping = rank_manager.get_replica_ranks_mapping()
        final_ranks = sorted([r.rank for r in mapping.values()])
        assert final_ranks == [0, 1, 2, 3, 4]

        # Verify no duplicates
        assert len(final_ranks) == len(set(final_ranks))

    def test_multiple_sequential_releases_reuse_in_order(self):
        """Test releasing multiple ranks in sequence maintains correct state."""
        rank_manager = DeploymentRankManager()

        # Assign ranks 0-5
        for i in range(6):
            rank_manager.assign_rank(f"r{i}", "node_1")

        # Release ranks 0, 2, 4
        rank_manager.release_rank("r0")
        rank_manager.release_rank("r2")
        rank_manager.release_rank("r4")

        # Assign new replicas - should reuse in ascending order (min first)
        new_rank1 = rank_manager.assign_rank("r6", "node_1")
        new_rank2 = rank_manager.assign_rank("r7", "node_1")
        new_rank3 = rank_manager.assign_rank("r8", "node_1")

        # Should reuse 0, 2, 4 in that order
        assert new_rank1.rank == 0
        assert new_rank2.rank == 2
        assert new_rank3.rank == 4

    def test_interleaved_assign_release_recover(self):
        """Test complex sequence of operations maintains consistency."""
        rank_manager = DeploymentRankManager()

        # Complex sequence
        rank_manager.assign_rank("r1", "node_1")  # rank 0
        rank_manager.assign_rank("r2", "node_1")  # rank 1
        rank_manager.release_rank("r1")  # release 0
        rank_manager.recover_rank(
            "r3", "node_2", ReplicaRank(rank=5, node_rank=1, local_rank=0)
        )
        rank_manager.assign_rank("r4", "node_1")  # should get 0 (reused)
        rank_manager.release_rank("r2")  # release 1
        rank_manager.assign_rank("r5", "node_2")  # should get 1 (reused)

        # Verify final state
        assert rank_manager.get_replica_rank("r4").rank == 0
        assert rank_manager.get_replica_rank("r5").rank == 1
        assert rank_manager.get_replica_rank("r3").rank == 5

    def test_release_all_then_reassign_all(self):
        """Test releasing all replicas then reassigning maintains correct state."""
        rank_manager = DeploymentRankManager()

        # Assign replicas across nodes
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.assign_rank("r2", "node_2")
        rank_manager.assign_rank("r3", "node_1")

        # Release all
        rank_manager.release_rank("r1")
        rank_manager.release_rank("r2")
        rank_manager.release_rank("r3")

        # Verify mapping is empty
        assert rank_manager.get_replica_ranks_mapping() == {}

        # Reassign new replicas - should reuse ranks 0, 1, 2
        new_rank1 = rank_manager.assign_rank("r4", "node_3")
        new_rank2 = rank_manager.assign_rank("r5", "node_3")
        new_rank3 = rank_manager.assign_rank("r6", "node_3")

        assert new_rank1.rank == 0
        assert new_rank2.rank == 1
        assert new_rank3.rank == 2

    def test_recover_rank_with_same_replica_different_rank(self):
        """Test that recovering the same replica_id twice raises an error."""
        rank_manager = DeploymentRankManager()

        # First recovery
        rank_manager.recover_rank(
            "r1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )

        # Try to recover same replica_id again - should raise error
        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.recover_rank(
                "r1", "node_1", ReplicaRank(rank=1, node_rank=0, local_rank=1)
            )

    def test_large_scale_rank_management(self):
        """Test with many nodes and many replicas per node."""
        rank_manager = DeploymentRankManager()

        num_nodes = 50
        replicas_per_node = 10
        total_replicas = num_nodes * replicas_per_node

        # Assign replicas
        replica_ids = []
        for node_idx in range(num_nodes):
            for replica_idx in range(replicas_per_node):
                replica_id = f"n{node_idx}_r{replica_idx}"
                replica_ids.append(replica_id)
                rank_manager.assign_rank(replica_id, f"node_{node_idx}")

        # Verify total count
        mapping = rank_manager.get_replica_ranks_mapping()
        assert len(mapping) == total_replicas

        # Verify global ranks are contiguous [0, total-1]
        global_ranks = sorted([r.rank for r in mapping.values()])
        assert global_ranks == list(range(total_replicas))

        # Verify node ranks are contiguous [0, num_nodes-1]
        node_ranks = sorted({r.node_rank for r in mapping.values()})
        assert node_ranks == list(range(num_nodes))

        # Verify local ranks per node
        for node_idx in range(num_nodes):
            node_replicas = [
                rid for rid in replica_ids if rid.startswith(f"n{node_idx}_")
            ]
            local_ranks = sorted([mapping[rid].local_rank for rid in node_replicas])
            assert local_ranks == list(range(replicas_per_node))

    def test_consistency_check_with_released_ranks_in_system(self):
        """Test consistency check works correctly when released_ranks exist."""
        rank_manager = DeploymentRankManager()

        replica1 = MockDeploymentReplica("r1")
        replica2 = MockDeploymentReplica("r2")
        replica3 = MockDeploymentReplica("r3")

        # Assign 5 replicas
        rank_manager.assign_rank("r1", "node_1")  # 0
        rank_manager.assign_rank("r2", "node_1")  # 1
        rank_manager.assign_rank("r3", "node_1")  # 2
        rank_manager.assign_rank("r4", "node_1")  # 3
        rank_manager.assign_rank("r5", "node_1")  # 4

        # Release two (creating gaps)
        rank_manager.release_rank("r4")  # Release 3
        rank_manager.release_rank("r5")  # Release 4

        # Check consistency with only remaining 3 replicas
        result = rank_manager.check_rank_consistency_and_reassign_minimally(
            [replica1, replica2, replica3]
        )

        # Should not need reassignment (ranks are already 0, 1, 2)
        assert result == []

        # Verify ranks are correct
        mapping = rank_manager.get_replica_ranks_mapping()
        assert sorted([r.rank for r in mapping.values()]) == [0, 1, 2]

    def test_has_replica_rank_returns_false_for_unassigned(self):
        """Test has_replica_rank returns False for various unassigned states."""
        rank_manager = DeploymentRankManager()

        # Completely unassigned replica
        assert not rank_manager.has_replica_rank("nonexistent")

        # Assign then release
        rank_manager.assign_rank("r1", "node_1")
        rank_manager.release_rank("r1")
        assert not rank_manager.has_replica_rank("r1")


class TestDeploymentRankManagerErrorHandling:
    """Test cases for DeploymentRankManager error handling with fail_on_rank_error flag.

    This test class can be easily removed in the future when the error handling
    feature is no longer needed.
    """

    def test_assign_rank_error_with_fail_on_rank_error_true(self):
        """Test that assign_rank raises exception when fail_on_rank_error=True."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=True)
        rank_manager.assign_rank("replica_1", "node_1")

        # Should raise RuntimeError for duplicate assignment
        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.assign_rank("replica_1", "node_1")

    def test_assign_rank_error_with_fail_on_rank_error_false(self):
        """Test that assign_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        rank_manager.assign_rank("replica_1", "node_1")

        # Should return safe default (ReplicaRank(rank=0)) instead of raising
        result = rank_manager.assign_rank("replica_1", "node_1")
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
        rank_manager.assign_rank("replica_1", "node_1")

        # Should raise RuntimeError for duplicate recovery
        with pytest.raises(RuntimeError, match="already assigned"):
            rank_manager.recover_rank(
                "replica_1", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
            )

    def test_recover_rank_error_with_fail_on_rank_error_false(self):
        """Test that recover_rank returns safe default when fail_on_rank_error=False."""
        rank_manager = DeploymentRankManager(fail_on_rank_error=False)
        rank_manager.assign_rank("replica_1", "node_1")

        # Should return None instead of raising
        result = rank_manager.recover_rank(
            "replica_1", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
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
        rank_manager.assign_rank("replica_1", "node_1")
        rank_manager.assign_rank("stale_replica", "node_1")

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
            "replica_1", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
        )
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=0, node_rank=0, local_rank=0)
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
        rank1 = rank_manager.assign_rank("replica_1", "node_1")
        assert rank1.rank == 0

        # Test normal get
        rank = rank_manager.get_replica_rank("replica_1")
        assert rank.rank == 0

        # Test normal release
        rank_manager.release_rank("replica_1")
        assert not rank_manager.has_replica_rank("replica_1")

        # Test normal recover
        rank_manager.recover_rank(
            "replica_2", "node_1", ReplicaRank(rank=5, node_rank=0, local_rank=0)
        )

        assert rank_manager.get_replica_rank("replica_2").rank == 5

        # Test normal consistency check
        replica2 = MockDeploymentReplica("replica_2")
        replica3 = MockDeploymentReplica("replica_3")
        rank_manager.assign_rank("replica_3", "node_1")

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
        rank = rank_manager.assign_rank("replica_1", "node_1")
        assert rank.rank == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
