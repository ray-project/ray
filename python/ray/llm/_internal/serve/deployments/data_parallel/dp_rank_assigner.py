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
