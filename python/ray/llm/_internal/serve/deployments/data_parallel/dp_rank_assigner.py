import asyncio

from ray import serve


@serve.deployment(num_replicas=1)
class DPRankAssigner:
    """
    Data Parallel Rank Assigner.

    This class is used to assign a rank to each replica in the data parallel
    deployment.
    """

    def __init__(self, dp_size: int):
        self.dp_size = dp_size
        self.dp_size_local = 8
        self.num_nodes = self.dp_size // self.dp_size_local
        self.lock = asyncio.Lock()
        self.dp_address = None
        self.dp_rpc_port = None
        self.master_info_event = asyncio.Event()
        self.node_to_avail_ranks = {}

    async def register(self, replica_ctx: "serve.context.ReplicaContext", node_id: str):
        async with self.lock:
            if not self.node_to_avail_ranks:
                self.node_to_avail_ranks[node_id] = list(range(1, self.dp_size_local))
                return 0
            elif node_id not in self.node_to_avail_ranks:
                node_rank = len(self.node_to_avail_ranks)
                assert node_rank < self.num_nodes
                rank = node_rank * self.dp_size_local
                self.node_to_avail_ranks[node_id] = list(
                    range(rank + 1, rank + self.dp_size_local)
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
