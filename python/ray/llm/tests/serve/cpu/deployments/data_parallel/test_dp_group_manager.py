import asyncio
import sys
from unittest.mock import MagicMock, patch

import pytest

from ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager import (
    DPGroupManager,
)
from ray.serve.schema import ReplicaRank


class TestGetDpRank:
    """Test _get_dp_rank calculations."""

    def test_single_node_first_replica(self):
        """First replica on a single node gets dp_rank=0."""
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=8
        )
        assert dp_rank == 0

    def test_single_node_last_replica(self):
        """Last replica on a single node gets dp_rank=7 for dp_group_size=8."""
        replica_rank = ReplicaRank(rank=7, node_rank=0, local_rank=7)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=8
        )
        assert dp_rank == 7

    def test_single_node_middle_replica(self):
        """Middle replica on a single node."""
        replica_rank = ReplicaRank(rank=3, node_rank=0, local_rank=3)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=8
        )
        assert dp_rank == 3

    def test_multi_node_single_group_first_node(self):
        """Multi-node single group: first node contributes ranks 0-7."""
        # dp_group_size=16 spans 2 nodes with dp_size_per_node=8
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=16, dp_size_per_node=8
        )
        assert dp_rank == 0

        replica_rank = ReplicaRank(rank=7, node_rank=0, local_rank=7)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=16, dp_size_per_node=8
        )
        assert dp_rank == 7

    def test_multi_node_single_group_second_node(self):
        """Multi-node single group: second node contributes ranks 8-15."""
        replica_rank = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=16, dp_size_per_node=8
        )
        assert dp_rank == 8

        replica_rank = ReplicaRank(rank=15, node_rank=1, local_rank=7)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=16, dp_size_per_node=8
        )
        assert dp_rank == 15

    def test_multi_node_multi_group_second_group(self):
        """Multi-node multi-group: second group on second node.

        With dp_group_size=8 and dp_size_per_node=8, each node is its own group.
        Node 1's replicas should have dp_ranks 0-7 (within their group).
        """
        replica_rank = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=8
        )
        assert dp_rank == 0  # First in group 1

        replica_rank = ReplicaRank(rank=15, node_rank=1, local_rank=7)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=8
        )
        assert dp_rank == 7  # Last in group 1

    def test_cross_node_group(self):
        """Group spans across nodes (dp_group_size > dp_size_per_node)."""
        # 4 replicas per node, group size of 8 spans 2 nodes
        replica_rank = ReplicaRank(rank=4, node_rank=1, local_rank=0)
        dp_rank = DPGroupManager._get_dp_rank(
            replica_rank, dp_group_size=8, dp_size_per_node=4
        )
        assert dp_rank == 4  # node_rank=1 * 4 + local_rank=0 = 4

    def test_multiple_groups_per_node(self):
        """Multiple groups on a single node (dp_group_size < dp_size_per_node).

        With dp_size_per_node=8 and dp_group_size=4:
        - local_rank 0-3 -> group 0, dp_ranks 0-3
        - local_rank 4-7 -> group 1, dp_ranks 0-3
        """
        # First group on node 0: local_rank 0-3
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            dp_rank = DPGroupManager._get_dp_rank(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            assert (
                dp_rank == local_rank
            ), f"local_rank={local_rank} should have dp_rank={local_rank}"

        # Second group on node 0: local_rank 4-7 -> dp_ranks 0-3
        for local_rank in range(4, 8):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            dp_rank = DPGroupManager._get_dp_rank(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            expected_dp_rank = local_rank % 4
            assert (
                dp_rank == expected_dp_rank
            ), f"local_rank={local_rank} should have dp_rank={expected_dp_rank}"


class TestGetGroupIndex:
    """Test _get_group_index calculations."""

    def test_single_group_all_same_index(self):
        """All replicas in a single group have group_index=0."""
        for local_rank in range(8):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            group_index = DPGroupManager._get_group_index(
                replica_rank, dp_group_size=8, dp_size_per_node=8
            )
            assert group_index == 0

    def test_multi_group_single_node_per_group(self):
        """Each node forms its own group."""
        # Node 0 -> group 0
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=8, dp_size_per_node=8
            )
            == 0
        )

        # Node 1 -> group 1
        replica_rank = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=8, dp_size_per_node=8
            )
            == 1
        )

        # Node 2 -> group 2
        replica_rank = ReplicaRank(rank=16, node_rank=2, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=8, dp_size_per_node=8
            )
            == 2
        )

    def test_multi_node_per_group(self):
        """Multiple nodes form a single group."""
        # dp_group_size=16, dp_size_per_node=8 -> 2 nodes per group

        # Node 0 and 1 -> group 0
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=16, dp_size_per_node=8
            )
            == 0
        )
        replica_rank = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=16, dp_size_per_node=8
            )
            == 0
        )

        # Node 2 and 3 -> group 1
        replica_rank = ReplicaRank(rank=16, node_rank=2, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=16, dp_size_per_node=8
            )
            == 1
        )
        replica_rank = ReplicaRank(rank=24, node_rank=3, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=16, dp_size_per_node=8
            )
            == 1
        )

    def test_multiple_groups_per_node(self):
        """Multiple groups on a single node (dp_group_size < dp_size_per_node).

        With dp_size_per_node=8 and dp_group_size=4:
        - local_rank 0-3 -> group 0
        - local_rank 4-7 -> group 1
        """
        # First group on node 0
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            group_index = DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            assert group_index == 0, f"local_rank={local_rank} should be in group 0"

        # Second group on node 0
        for local_rank in range(4, 8):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            group_index = DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            assert group_index == 1, f"local_rank={local_rank} should be in group 1"

    def test_multiple_groups_per_node_multi_node(self):
        """Multiple groups per node across multiple nodes.

        With dp_size_per_node=8 and dp_group_size=4 on 2 nodes:
        - Node 0, local_rank 0-3 -> group 0
        - Node 0, local_rank 4-7 -> group 1
        - Node 1, local_rank 0-3 -> group 2
        - Node 1, local_rank 4-7 -> group 3
        """
        # Node 0, group 0
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            == 0
        )

        # Node 0, group 1
        replica_rank = ReplicaRank(rank=4, node_rank=0, local_rank=4)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            == 1
        )

        # Node 1, group 2
        replica_rank = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            == 2
        )

        # Node 1, group 3
        replica_rank = ReplicaRank(rank=12, node_rank=1, local_rank=4)
        assert (
            DPGroupManager._get_group_index(
                replica_rank, dp_group_size=4, dp_size_per_node=8
            )
            == 3
        )


class TestRegister:
    """Test register() method."""

    @pytest.fixture
    def manager(self):
        """Create a DPGroupManager instance for testing."""
        return DPGroupManager(dp_group_size=8, dp_size_per_node=8)

    @pytest.mark.asyncio
    async def test_first_time_registration(self, manager):
        """First-time registration proceeds without error."""
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        dp_rank, group_index = await manager.register(
            replica_rank=replica_rank,
            replica_id="replica-0",
        )
        assert dp_rank == 0
        assert group_index == 0

    @pytest.mark.asyncio
    async def test_register_multiple_replicas_same_group(self, manager):
        """Multiple replicas can register to the same group."""
        for local_rank in range(8):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            dp_rank, group_index = await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-{local_rank}",
            )
            assert dp_rank == local_rank
            assert group_index == 0

    @pytest.mark.asyncio
    async def test_register_multiple_groups(self, manager):
        """Multiple groups can register independently."""
        # Group 0 (node 0)
        replica_rank_0 = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        dp_rank_0, group_index_0 = await manager.register(
            replica_rank=replica_rank_0,
            replica_id="replica-0",
        )

        # Group 1 (node 1)
        replica_rank_1 = ReplicaRank(rank=8, node_rank=1, local_rank=0)
        dp_rank_1, group_index_1 = await manager.register(
            replica_rank=replica_rank_1,
            replica_id="replica-8",
        )

        # Both should be dp_rank=0 in their respective groups
        assert dp_rank_0 == 0
        assert dp_rank_1 == 0
        assert group_index_0 == 0
        assert group_index_1 == 1

        # Verify both groups exist
        assert 0 in manager._group_info
        assert 1 in manager._group_info

    @pytest.mark.asyncio
    async def test_register_tracks_replica_id(self, manager):
        """Registration stores replica_id in group_info."""
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        await manager.register(
            replica_rank=replica_rank,
            replica_id="replica-abc",
        )

        group = manager._group_info[0]
        assert group.dp_rank_to_replica_id[0] == "replica-abc"

    @pytest.mark.asyncio
    async def test_register_multiple_groups_per_node(self):
        """Multiple groups on a single node register independently.

        With dp_size_per_node=8 and dp_group_size=4:
        - local_rank 0-3 -> group 0, dp_ranks 0-3
        - local_rank 4-7 -> group 1, dp_ranks 0-3
        """
        # Create manager with dp_group_size=4 for this test
        manager = DPGroupManager(dp_group_size=4, dp_size_per_node=8)

        # Register replicas for group 0 (local_rank 0-3)
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            dp_rank, group_index = await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-{local_rank}",
            )
            assert dp_rank == local_rank
            assert group_index == 0

        # Register replicas for group 1 (local_rank 4-7)
        for local_rank in range(4, 8):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            dp_rank, group_index = await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-{local_rank}",
            )
            expected_dp_rank = local_rank % 4
            assert dp_rank == expected_dp_rank
            assert group_index == 1

        # Verify both groups exist and have correct members
        assert 0 in manager._group_info
        assert 1 in manager._group_info

        # Group 0 has dp_ranks 0-3
        assert set(manager._group_info[0].dp_rank_to_replica_id.keys()) == {0, 1, 2, 3}
        # Group 1 has dp_ranks 0-3
        assert set(manager._group_info[1].dp_rank_to_replica_id.keys()) == {0, 1, 2, 3}


class TestMasterInfo:
    """Test set_dp_master_info() and get_dp_master_info()."""

    @pytest.fixture
    def manager(self):
        """Create a DPGroupManager instance for testing."""
        return DPGroupManager(dp_group_size=8, dp_size_per_node=8)

    @pytest.mark.asyncio
    async def test_set_and_get_master_info(self, manager):
        """Master info can be set and retrieved for a group."""
        await manager.set_dp_master_info(
            group_index=0, dp_address="192.168.1.1", dp_rpc_port=12345
        )

        address, port = await manager.get_dp_master_info(group_index=0)
        assert address == "192.168.1.1"
        assert port == 12345

    @pytest.mark.asyncio
    async def test_get_master_info_waits_for_set(self, manager):
        """get_dp_master_info blocks until master info is available."""
        results = []

        async def getter():
            result = await manager.get_dp_master_info(group_index=0)
            results.append(result)

        async def setter():
            await asyncio.sleep(0.1)  # Delay the set
            await manager.set_dp_master_info(
                group_index=0, dp_address="10.0.0.1", dp_rpc_port=9999
            )

        # Start both concurrently
        await asyncio.gather(getter(), setter())

        assert len(results) == 1
        assert results[0] == ("10.0.0.1", 9999)

    @pytest.mark.asyncio
    async def test_multiple_waiters(self, manager):
        """Multiple callers can wait for and receive master info."""
        results = []

        async def getter(waiter_id):
            result = await manager.get_dp_master_info(group_index=0)
            results.append((waiter_id, result))

        async def setter():
            await asyncio.sleep(0.1)
            await manager.set_dp_master_info(
                group_index=0, dp_address="10.0.0.2", dp_rpc_port=8888
            )

        # Multiple getters waiting
        await asyncio.gather(getter(1), getter(2), getter(3), setter())

        assert len(results) == 3
        for waiter_id, result in results:
            assert result == ("10.0.0.2", 8888)

    @pytest.mark.asyncio
    async def test_independent_groups(self, manager):
        """Master info is independent per group."""
        await manager.set_dp_master_info(
            group_index=0, dp_address="192.168.0.1", dp_rpc_port=1111
        )
        await manager.set_dp_master_info(
            group_index=1, dp_address="192.168.0.2", dp_rpc_port=2222
        )

        addr_0, port_0 = await manager.get_dp_master_info(group_index=0)
        addr_1, port_1 = await manager.get_dp_master_info(group_index=1)

        assert (addr_0, port_0) == ("192.168.0.1", 1111)
        assert (addr_1, port_1) == ("192.168.0.2", 2222)


class TestDoubleRegistration:
    """Test double-registration detection and group kill logic."""

    @pytest.fixture
    def manager(self):
        """Create a DPGroupManager instance for testing."""
        return DPGroupManager(dp_group_size=8, dp_size_per_node=8)

    @pytest.mark.asyncio
    async def test_double_registration_triggers_kill(self, manager):
        """Double registration initiates group kill."""
        # First registration
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        await manager.register(
            replica_rank=replica_rank,
            replica_id="replica-original",
        )

        # Verify original is registered
        assert manager._group_info[0].dp_rank_to_replica_id[0] == "replica-original"

        # Mock ray.get_actor and ray.kill to track calls
        with patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.get_actor"
        ) as mock_get_actor, patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.kill"
        ) as mock_kill:
            mock_actor = MagicMock()
            mock_get_actor.return_value = mock_actor

            # Double registration with a new replica claiming the same dp_rank
            await manager.register(
                replica_rank=replica_rank,
                replica_id="replica-new",
            )

            # Verify kill was called on the original replica
            mock_get_actor.assert_called_once_with("replica-original")
            mock_kill.assert_called_once_with(mock_actor, no_restart=False)

        # Verify new replica is now registered
        assert manager._group_info[0].dp_rank_to_replica_id[0] == "replica-new"

    @pytest.mark.asyncio
    async def test_double_registration_clears_group_state(self, manager):
        """Group state is cleared before kills to prevent cascading."""
        # Register multiple replicas in the same group
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-{local_rank}",
            )

        # Set master info
        await manager.set_dp_master_info(
            group_index=0, dp_address="192.168.1.1", dp_rpc_port=12345
        )

        # Verify all are registered and master info is set
        assert len(manager._group_info[0].dp_rank_to_replica_id) == 4
        assert manager._group_info[0].master_info == ("192.168.1.1", 12345)

        with patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.get_actor"
        ), patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.kill"
        ):
            # Double registration for dp_rank=0
            replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
            await manager.register(
                replica_rank=replica_rank,
                replica_id="replica-new-0",
            )

        # Verify group state was reset (only the new replica should be registered)
        assert len(manager._group_info[0].dp_rank_to_replica_id) == 1
        assert manager._group_info[0].dp_rank_to_replica_id[0] == "replica-new-0"
        # Master info should be cleared for new election
        assert manager._group_info[0].master_info is None

    @pytest.mark.asyncio
    async def test_only_affected_group_is_reset(self, manager):
        """Only the affected group is restarted, other groups unaffected."""
        # Register replicas in group 0 (node 0)
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=local_rank, node_rank=0, local_rank=local_rank
            )
            await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-g0-{local_rank}",
            )
        await manager.set_dp_master_info(
            group_index=0, dp_address="192.168.0.1", dp_rpc_port=1111
        )

        # Register replicas in group 1 (node 1)
        for local_rank in range(4):
            replica_rank = ReplicaRank(
                rank=8 + local_rank, node_rank=1, local_rank=local_rank
            )
            await manager.register(
                replica_rank=replica_rank,
                replica_id=f"replica-g1-{local_rank}",
            )
        await manager.set_dp_master_info(
            group_index=1, dp_address="192.168.0.2", dp_rpc_port=2222
        )

        # Verify both groups are set up
        assert len(manager._group_info[0].dp_rank_to_replica_id) == 4
        assert len(manager._group_info[1].dp_rank_to_replica_id) == 4

        with patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.get_actor"
        ), patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.kill"
        ):
            # Double registration in group 0 only
            replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
            await manager.register(
                replica_rank=replica_rank,
                replica_id="replica-g0-new",
            )

        # Group 0 should be reset
        assert len(manager._group_info[0].dp_rank_to_replica_id) == 1
        assert manager._group_info[0].master_info is None

        # Group 1 should be unaffected
        assert len(manager._group_info[1].dp_rank_to_replica_id) == 4
        assert manager._group_info[1].master_info == ("192.168.0.2", 2222)

    @pytest.mark.asyncio
    async def test_already_dead_replica_handled_gracefully(self, manager):
        """Already-dead replicas are skipped without error."""
        # Register a replica
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)
        await manager.register(
            replica_rank=replica_rank,
            replica_id="replica-dead",
        )

        with patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.get_actor"
        ) as mock_get_actor:
            # Simulate actor not found (already dead)
            mock_get_actor.side_effect = ValueError("Actor not found")

            # Double registration should succeed without error
            dp_rank, group_index = await manager.register(
                replica_rank=replica_rank,
                replica_id="replica-new",
            )

            assert dp_rank == 0
            assert group_index == 0
            assert manager._group_info[0].dp_rank_to_replica_id[0] == "replica-new"

    @pytest.mark.asyncio
    async def test_same_replica_re_registration_no_kill(self, manager):
        """Same replica re-registering does not trigger kill."""
        replica_rank = ReplicaRank(rank=0, node_rank=0, local_rank=0)

        # First registration
        await manager.register(
            replica_rank=replica_rank,
            replica_id="replica-same",
        )

        with patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.get_actor"
        ) as mock_get_actor, patch(
            "ray.llm._internal.serve.serving_patterns.data_parallel.dp_group_manager.ray.kill"
        ) as mock_kill:
            # Same replica re-registering (e.g., reconnection)
            await manager.register(
                replica_rank=replica_rank,
                replica_id="replica-same",
            )

            # No kill should be triggered
            mock_get_actor.assert_not_called()
            mock_kill.assert_not_called()

        # Replica should still be registered
        assert manager._group_info[0].dp_rank_to_replica_id[0] == "replica-same"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
