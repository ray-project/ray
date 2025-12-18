import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from ray import serve
from ray.serve.schema import ReplicaRank

logger = logging.getLogger(__name__)


@dataclass
class GroupInfo:
    """State for a single DP group.

    Attributes:
        master_info: Tuple of (dp_address, dp_rpc_port) for the group's master.
        dp_rank_to_replica_id: Mapping from DP rank to replica ID for tracking
            registered replicas.
        master_info_event: Event that is set when master info is available.
    """

    master_info: Optional[Tuple[str, int]] = None
    dp_rank_to_replica_id: Dict[int, str] = field(default_factory=dict)
    master_info_event: asyncio.Event = field(default_factory=asyncio.Event)


@serve.deployment(num_replicas=1)
class DPGroupManager:
    """Data Parallel Group Manager.

    This actor manages multiple DP groups within a single deployment. Each group
    is a set of replicas that participate in the same collective operations.

    Unlike the legacy _DPRankAssigner which supports only a single global DP group,
    DPGroupManager:
    - Supports multiple DP groups per deployment
    - Uses deterministic rank assignment based on ReplicaRank (not incremental)
    - Tracks per-group master info (ip/port)
    - Enables per-group failure isolation (Milestone 3)

    Key concepts:
    - dp_group_size: Number of replicas in each DP group (e.g., 8)
    - dp_size_per_node: Number of DP replicas per node (e.g., 8 for single-node groups)
    - group_index: Which group a replica belongs to (0, 1, 2, ...)
    - dp_rank: Rank within the group (0 to dp_group_size-1)

    Example:
        With 16 total replicas, dp_group_size=8, dp_size_per_node=8:
        - Group 0: replicas with global ranks 0-7 (all on node 0)
        - Group 1: replicas with global ranks 8-15 (all on node 1)

    State:
        group_info: group_index → GroupInfo containing:
            - master_info: (dp_address, dp_rpc_port) for the group
            - dp_rank_to_replica_id: {dp_rank -> replica_id} for tracking
    """

    def __init__(self):
        self._group_info: Dict[int, GroupInfo] = {}
        self._group_info_lock = asyncio.Lock()
        logger.info("DPGroupManager initialized")

    @staticmethod
    def _get_dp_rank(
        replica_rank: ReplicaRank,
        dp_group_size: int,
        dp_size_per_node: int,
    ) -> int:
        """Calculate the DP rank within a group from the replica's rank info.

        The DP rank is the position within the DP group, ranging from 0 to
        dp_group_size - 1.

        Formula: (node_rank * dp_size_per_node + local_rank) % dp_group_size

        Args:
            replica_rank: The replica's rank info containing node_rank and local_rank.
            dp_group_size: Total number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas per node.

        Returns:
            The DP rank within the group (0 to dp_group_size - 1).

        Examples:
            Single node (dp_group_size=8, dp_size_per_node=8):
                node_rank=0, local_rank=0 -> dp_rank=0
                node_rank=0, local_rank=7 -> dp_rank=7

            Multi-node, single group (dp_group_size=16, dp_size_per_node=8):
                node_rank=0, local_rank=0 -> dp_rank=0
                node_rank=1, local_rank=0 -> dp_rank=8
                node_rank=1, local_rank=7 -> dp_rank=15

            Multi-node, multi-group (dp_group_size=8, dp_size_per_node=8):
                node_rank=0, local_rank=0 -> dp_rank=0 (group 0)
                node_rank=1, local_rank=0 -> dp_rank=0 (group 1)
        """
        global_rank = (
            replica_rank.node_rank * dp_size_per_node + replica_rank.local_rank
        )
        return global_rank % dp_group_size

    @staticmethod
    def _get_group_index(
        replica_rank: ReplicaRank,
        dp_group_size: int,
        dp_size_per_node: int,
    ) -> int:
        """Calculate which DP group a replica belongs to.

        Args:
            replica_rank: The replica's rank info containing node_rank and local_rank.
            dp_group_size: Total number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas per node.

        Returns:
            The group index (0, 1, 2, ...).

        Examples:
            Single node per group (dp_group_size=8, dp_size_per_node=8):
                node_rank=0 -> group_index=0
                node_rank=1 -> group_index=1

            Two nodes per group (dp_group_size=16, dp_size_per_node=8):
                node_rank=0, node_rank=1 -> group_index=0
                node_rank=2, node_rank=3 -> group_index=1
        """
        global_rank = (
            replica_rank.node_rank * dp_size_per_node + replica_rank.local_rank
        )
        return global_rank // dp_group_size

    async def register(
        self,
        replica_rank: ReplicaRank,
        replica_id: str,
        dp_group_size: int,
        dp_size_per_node: int,
    ) -> int:
        """Register a replica to its DP group.

        This is the first-time registration flow. The replica is added to its
        group's tracking set. Double-registration detection and group restart
        logic will be added in Milestone 3.

        Args:
            replica_rank: The replica's rank info from serve.get_replica_context().rank.
            replica_id: Unique identifier for the replica.
            dp_group_size: Total number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas per node.

        Returns:
            The DP rank within the group (0 to dp_group_size - 1).
        """
        dp_rank = self._get_dp_rank(replica_rank, dp_group_size, dp_size_per_node)
        group_index = self._get_group_index(
            replica_rank, dp_group_size, dp_size_per_node
        )

        async with self._group_info_lock:
            if group_index not in self._group_info:
                self._group_info[group_index] = GroupInfo()

            group = self._group_info[group_index]
            group.dp_rank_to_replica_id[dp_rank] = replica_id

            logger.info(
                f"Registered replica {replica_id} to group {group_index} "
                f"with dp_rank {dp_rank} (replica_rank={replica_rank})"
            )

        return dp_rank

    async def set_dp_master_info(
        self,
        group_index: int,
        dp_address: str,
        dp_rpc_port: int,
    ) -> None:
        """Set the master info for a DP group.

        Called by dp_rank=0 replica after obtaining its address and port.

        Args:
            group_index: The index of the DP group.
            dp_address: The IP address of the master.
            dp_rpc_port: The RPC port of the master.
        """
        async with self._group_info_lock:
            if group_index not in self._group_info:
                self._group_info[group_index] = GroupInfo()

            group = self._group_info[group_index]
            group.master_info = (dp_address, dp_rpc_port)

            # Notify all waiters that the master info is available
            group.master_info_event.set()

            logger.info(
                f"Set master info for group {group_index}: "
                f"address={dp_address}, port={dp_rpc_port}"
            )

    async def get_dp_master_info(
        self,
        group_index: int,
    ) -> Tuple[str, int]:
        """Get the master info for a DP group.

        Blocks until the master info is available (set by dp_rank=0).

        Args:
            group_index: The index of the DP group.

        Returns:
            Tuple of (dp_address, dp_rpc_port).
        """
        # Ensure the group exists before waiting
        async with self._group_info_lock:
            if group_index not in self._group_info:
                self._group_info[group_index] = GroupInfo()
            event = self._group_info[group_index].master_info_event

        # Wait outside the lock to avoid blocking other operations
        await event.wait()

        async with self._group_info_lock:
            return self._group_info[group_index].master_info
