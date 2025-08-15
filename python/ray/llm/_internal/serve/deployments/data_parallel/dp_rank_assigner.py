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
        self.lock = asyncio.Lock()
        self.next_rank = 0
        self.dp_address = None
        self.dp_rpc_port = None
        self.master_info_event = asyncio.Event()

    async def register(self, replica_ctx: "serve.context.ReplicaContext"):
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

    async def set_dp_master_info(self, dp_address: str, dp_rpc_port: int):
        self.dp_address = dp_address
        self.dp_rpc_port = dp_rpc_port
        self.master_info_event.set()

    async def get_dp_master_info(self):
        await self.master_info_event.wait()
        return self.dp_address, self.dp_rpc_port
