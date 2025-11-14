import asyncio
import logging
from typing import Dict, List, Optional

from ray import serve

logger = logging.getLogger(__name__)


@serve.deployment(num_replicas=1)
class _DPRankAssigner:
    """
    Data Parallel Rank Assigner.

    This class is used to assign a rank to each replica in the data parallel
    deployment.
    """

    def __init__(self, dp_size: int, dp_size_per_node: Optional[int] = None):
        self.dp_size: int = dp_size
        self.dp_size_per_node: Optional[int] = dp_size_per_node
        self.lock: asyncio.Lock = asyncio.Lock()
        self.dp_address: Optional[str] = None
        self.dp_rpc_port: Optional[int] = None
        self.master_info_event: asyncio.Event = asyncio.Event()

        # Fields for _register_random_placement():
        # Next rank to assign
        self.next_rank: Optional[int] = None

        # Fields for _register_node_pack_placement():
        # Number of nodes to assign to
        self.num_nodes: Optional[int] = None
        # Map from node id to available ranks
        self.node_to_avail_ranks: Dict[str, List[int]] = {}

        if dp_size_per_node is None:
            self.next_rank = 0
            logger.info(
                f"Using random placement rank assigner for DP size {self.dp_size}"
            )
        else:
            if self.dp_size_per_node <= 0:
                raise ValueError(
                    f"dp_size_per_node {self.dp_size_per_node} must be greater than 0"
                )
            if self.dp_size % self.dp_size_per_node != 0:
                raise ValueError(
                    f"dp_size {self.dp_size} must be divisible by dp_size_per_node {self.dp_size_per_node}"
                )
            self.num_nodes = self.dp_size // self.dp_size_per_node
            logger.info(
                f"Using node pack placement rank assigner for DP size {self.dp_size}"
                f"with dp_size_per_node {self.dp_size_per_node}"
            )

    async def register(self, node_id: Optional[str] = None):
        """
        Register a replica and assign a rank to it.

        Args:
            node_id: The node id of the replica.

        Returns:
            The rank of the replica.
        """
        if self.dp_size_per_node is None:
            return await self._register_random_placement()
        else:
            if node_id is None:
                raise ValueError("node_id is required for node pack placement")
            return await self._register_node_pack_placement(node_id)

    async def _register_random_placement(self):
        """
        Assign a rank based on random placement.

        The ranks are assigned in a random order, regardless of its node id.
        """
        async with self.lock:
            if self.next_rank >= self.dp_size:
                raise ValueError(
                    f"Attempted to assign rank {self.next_rank} but dp_size is {self.dp_size}"
                )
            # TODO(rui): instead of using the naive increment approach,
            # we should use the Ray Serve Replica Rank API to assign ranks.
            rank = self.next_rank
            self.next_rank += 1
            return rank

    async def _register_node_pack_placement(self, node_id: str):
        """
        Assign a rank based on node pack placement.

        This should be used for DeepEP which assumes that the ranks ranging from
        [dp_rank_per_node * node_rank, dp_rank_per_node * (node_rank + 1) - 1] are
        assigned to the same node.

        For example, if dp_size_per_node is 8, and there are 16 ranks in total, then
        the ranks [0, 7] should be assigned to one node, and ranks [8, 15] should be
        assigned to another node.
        """
        async with self.lock:
            if not self.node_to_avail_ranks:
                self.node_to_avail_ranks[node_id] = list(
                    range(1, self.dp_size_per_node)
                )
                return 0
            elif node_id not in self.node_to_avail_ranks:
                node_rank = len(self.node_to_avail_ranks)
                assert node_rank < self.num_nodes
                rank = node_rank * self.dp_size_per_node
                self.node_to_avail_ranks[node_id] = list(
                    range(rank + 1, rank + self.dp_size_per_node)
                )
                return rank
            else:
                rank = self.node_to_avail_ranks[node_id].pop(0)
                return rank

    async def set_dp_master_info(self, dp_address: str, dp_rpc_port: int):
        self.dp_address = dp_address
        self.dp_rpc_port = dp_rpc_port
        self.master_info_event.set()

    async def get_dp_master_info(self):
        await self.master_info_event.wait()
        return self.dp_address, self.dp_rpc_port
