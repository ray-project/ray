import asyncio

from ray import serve


@serve.deployment(num_replicas=1)
class DPRankAssigner:
    def __init__(self, dp_size: int):
        self.dp_size = dp_size
        self.lock = asyncio.Lock()
        self.next_rank = 0

    async def register(self, replica_ctx: "serve.context.ReplicaContext"):
        async with self.lock:
            rank = self.next_rank
            self.next_rank += 1
            return rank
