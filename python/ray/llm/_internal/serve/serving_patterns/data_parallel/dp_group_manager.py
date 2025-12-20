import asyncio
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import ray
from ray.serve.schema import ReplicaRank

logger = logging.getLogger(__name__)


@dataclass
class GroupInfo:
    """State for a single DP group.

    Attributes:
        master_info: Tuple of (dp_address, dp_rpc_port) for the group's master.
        dp_rank_to_replica_id: Mapping from DP rank to replica ID for tracking
            registered replicas.

    Note:
        This class is kept serializable (no asyncio.Event). The master_info_event
        is stored separately in DPGroupManager.
    """

    master_info: Optional[Tuple[str, int]] = None
    dp_rank_to_replica_id: Dict[int, str] = field(default_factory=dict)


class DPGroupManager:
    """Data Parallel Group Manager.

    This actor manages multiple DP groups within a single deployment. Each group
    is a set of replicas that participate in the same collective operations.

    Key concepts:
    - dp_group_size: Number of replicas in each DP group (e.g., 8)
    - dp_size_per_node: Number of DP replicas per node, calculated as
        (node GPU count) / (TP size). This equals the number of replicas per node.
        (e.g., 8-GPU node with TP=2 -> 4 replicas per node)
    - group_index: Which group a replica belongs to (0 to num_replicas // dp_group_size - 1)
    - dp_rank: Rank within the group (0 to dp_group_size-1)

    Example:
        With 4 nodes, 16 total TP=2 replicas (4 replicas per 8-GPU node), and 2 DP groups:
        - dp_group_size=8, dp_size_per_node=4
        - Group 0: replicas with global ranks 0-7 (nodes 0-1, 4 replicas each)
        - Group 1: replicas with global ranks 8-15 (nodes 2-3, 4 replicas each)

    State:
        group_info: group_index → GroupInfo containing:
            - master_info: (dp_address, dp_rpc_port) for the group
            - dp_rank_to_replica_id: {dp_rank -> replica_id} for registration tracking
    """

    def __init__(self, dp_group_size: int, dp_size_per_node: int):
        """Initialize the DPGroupManager.

        Args:
            dp_group_size: Number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas that fit on a node
                (node GPU count / TP size).
        """
        self.dp_group_size = dp_group_size
        self.dp_size_per_node = dp_size_per_node
        self._group_info: Dict[int, GroupInfo] = {}
        self._master_info_events: Dict[int, asyncio.Event] = {}
        self._group_info_lock = asyncio.Lock()
        logger.info(
            f"DPGroupManager initialized with dp_group_size={dp_group_size}, "
            f"dp_size_per_node={dp_size_per_node}"
        )

    @staticmethod
    def _get_dp_rank(
        replica_rank: ReplicaRank,
        dp_group_size: int,
        dp_size_per_node: int,
    ) -> int:
        """Calculate the DP rank within a group.

        Uses dp_size_per_node to compute a deterministic global rank from
        (node_rank, local_rank), then takes modulo to get the rank within
        the group.

        Args:
            replica_rank: The replica's rank info containing node_rank and local_rank.
            dp_group_size: Number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas that fit on a node.

        Returns:
            The DP rank within the group (0 to dp_group_size - 1).
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

        Uses dp_size_per_node to compute a deterministic global rank from
        (node_rank, local_rank), then divides by group size to get the
        group index.

        Args:
            replica_rank: The replica's rank info containing node_rank and local_rank.
            dp_group_size: Number of replicas in each DP group.
            dp_size_per_node: Number of DP replicas that fit on a node.

        Returns:
            The group index (0, 1, 2, ...).
        """
        global_rank = (
            replica_rank.node_rank * dp_size_per_node + replica_rank.local_rank
        )
        return global_rank // dp_group_size

    async def register(
        self,
        replica_rank: ReplicaRank,
        replica_id: str,
    ) -> Tuple[int, int]:
        """Register a replica to its DP group.

        Detects double-registration, which indicates that a DP rank replica has
        died. Registration is tracked uniquely by (node_rank, local_rank, replica_id).
        When a replica dies, it will try to register to the same (node_rank, local_rank),
        but with a different replica_id.

        When double-registration is detected, the DPGroupManager kills all
        replicas in the group to trigger a clean restart.

        Args:
            replica_rank: The replica's rank info from serve.get_replica_context().rank.
            replica_id: Unique identifier for the replica.

        Returns:
            Tuple of (dp_rank, group_index).
        """
        dp_rank = self._get_dp_rank(
            replica_rank, self.dp_group_size, self.dp_size_per_node
        )
        group_index = self._get_group_index(
            replica_rank, self.dp_group_size, self.dp_size_per_node
        )

        async with self._group_info_lock:
            if group_index not in self._group_info:
                self._group_info[group_index] = GroupInfo()
                self._master_info_events[group_index] = asyncio.Event()

            group = self._group_info[group_index]

            # Get replicas to kill if double-registration is detected
            replicas_to_kill = self._handle_double_registration(
                group, group_index, dp_rank, replica_id
            )

            # Register the new replica
            group.dp_rank_to_replica_id[dp_rank] = replica_id

            logger.info(
                f"Registered replica {replica_id} to group {group_index} "
                f"with dp_rank {dp_rank} (replica_rank={replica_rank})"
            )

        # Kill replicas outside the lock to avoid blocking
        if replicas_to_kill:
            await self._kill_replicas(replicas_to_kill, group_index)

        return dp_rank, group_index

    def _reset_group(self, group: GroupInfo, group_index: int) -> None:
        """Reset a group's state for a fresh restart.

        Clears the dp_rank_to_replica_id mapping and resets the master info
        so a new master can be elected.

        Must be called while holding _group_info_lock.
        """
        assert (
            group_index in self._master_info_events
        ), f"Attempted to reset group {group_index}, but no master_info_event exists."
        event = self._master_info_events[group_index]
        event.set()
        event.clear()

        group.dp_rank_to_replica_id.clear()
        group.master_info = None

    def _handle_double_registration(
        self,
        group: GroupInfo,
        group_index: int,
        dp_rank: int,
        replica_id: str,
    ) -> List[str]:
        """Check for and handle double-registration.

        If a new replica is trying to register for a DP rank already occupied
        by a different replica, this indicates a replica failure/restart.
        Collects replicas to kill and resets the group for a clean restart.

        Must be called while holding _group_info_lock.

        Args:
            group: The group info object.
            group_index: The index of the group.
            dp_rank: The data parallel rank being registered.
            replica_id: The replica ID attempting to register.

        Returns:
            List of replica IDs to kill (empty if no double-registration).
        """
        existing_replica_id = group.dp_rank_to_replica_id.get(dp_rank)
        if existing_replica_id is None:
            return []

        # Same replica_id should never re-register
        assert existing_replica_id != replica_id, (
            f"Replica {replica_id} attempted to register twice for "
            f"group {group_index}, dp_rank {dp_rank}. This should never happen."
        )

        logger.warning(
            f"Double-registration detected for group {group_index}, "
            f"dp_rank {dp_rank}: existing={existing_replica_id}, "
            f"new={replica_id}. Killing all replicas in the group."
        )

        # Collect all replica IDs to kill (excluding the new registrant)
        replicas_to_kill = [
            rid for rid in group.dp_rank_to_replica_id.values() if rid != replica_id
        ]

        # Clear the group state BEFORE killing replicas to prevent cascading kills.
        # If we killed first, other replicas might detect the kill and try to
        # re-register, triggering another round of double-registration detection.
        self._reset_group(group, group_index)

        return replicas_to_kill

    async def _kill_replica(self, replica_id: str, group_index: int) -> None:
        """Kill a replica actor.

        Args:
            replica_id: The replica ID to kill.
            group_index: The group index (for logging).

        Note:
            We use no_restart=False to allow Ray Serve to restart the replica.
            The restarted replica will re-register with a fresh state.
        """
        try:
            actor = ray.get_actor(replica_id)
            ray.kill(actor, no_restart=False)
            logger.info(f"Killed replica {replica_id} from group {group_index}")
        except ValueError:
            # Actor not found - already dead
            logger.info(
                f"Replica {replica_id} from group {group_index} "
                "already dead, skipping kill"
            )
        except Exception as e:
            logger.warning(
                f"Failed to kill replica {replica_id} from group " f"{group_index}: {e}"
            )

    async def _kill_replicas(self, replica_ids: List[str], group_index: int) -> None:
        """Kill all replicas in the given list."""
        await asyncio.gather(
            *[self._kill_replica(rid, group_index) for rid in replica_ids]
        )

    async def set_dp_master_info(
        self,
        group_index: int,
        dp_address: str,
        dp_rpc_port: int,
    ) -> None:
        """Set the master info for a DP group.

        Called by dp_rank=0 replica after obtaining its address and port.
        The group must already exist (i.e., register() must be called first).

        Args:
            group_index: The index of the DP group.
            dp_address: The IP address of the master.
            dp_rpc_port: The RPC port of the master.
        """
        async with self._group_info_lock:
            assert group_index in self._group_info, (
                f"Group {group_index} does not exist. "
                "register() must be called before set_dp_master_info()."
            )

            group = self._group_info[group_index]
            group.master_info = (dp_address, dp_rpc_port)

            # Notify all waiters that the master info is available
            self._master_info_events[group_index].set()

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
        The group must already exist (i.e., register() must be called first).

        The while True loop handles a race condition: if master_info is reset
        (due to group failure) between event.wait() returning and acquiring
        the lock, we need to wait again for the new master to set its info.
        The loop ensures we always return valid master_info.

        Args:
            group_index: The index of the DP group.

        Returns:
            Tuple of (dp_address, dp_rpc_port).
        """
        while True:
            async with self._group_info_lock:
                assert group_index in self._group_info, (
                    f"Group {group_index} does not exist. "
                    "register() must be called before get_dp_master_info()."
                )

                group = self._group_info[group_index]

                if group.master_info is not None:
                    # Master info may be ready right away
                    return group.master_info

                event = self._master_info_events[group_index]

            # Otherwise wait
            await event.wait()
