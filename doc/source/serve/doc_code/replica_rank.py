# __replica_rank_start__
from ray import serve


@serve.deployment(num_replicas=4)
class ModelShard:
    def __call__(self):
        return {
            "rank": serve.get_replica_context().rank,
            "world_size": serve.get_replica_context().world_size,
        }


app = ModelShard.bind()
# __replica_rank_end__

# __reconfigure_rank_start__
from typing import Any
from ray import serve


@serve.deployment(num_replicas=4, user_config={"name": "model_v1"})
class RankAwareModel:
    def __init__(self):
        context = serve.get_replica_context()
        self.rank = context.rank
        self.world_size = context.world_size
        self.model_name = None
        print(f"Replica rank: {self.rank}/{self.world_size}")

    async def reconfigure(self, user_config: Any, rank: int):
        """Called when user_config or rank changes."""
        self.rank = rank
        self.world_size = serve.get_replica_context().world_size
        self.model_name = user_config.get("name")
        print(f"Reconfigured: rank={self.rank}, model={self.model_name}")

    def __call__(self):
        return {"rank": self.rank, "model_name": self.model_name}


app2 = RankAwareModel.bind()
# __reconfigure_rank_end__

if __name__ == "__main__":
    h = serve.run(app)
    # Test that we can get rank information from replicas
    seen_ranks = set()
    for _ in range(20):
        res = h.remote().result()
        assert res["rank"] in [0, 1, 2, 3]
        assert res["world_size"] == 4
        seen_ranks.add(res["rank"])

    # Verify we hit all replicas
    print(f"Saw ranks: {sorted(seen_ranks)}")

    h = serve.run(app2)
    for _ in range(20):
        res = h.remote().result()
        assert res["rank"] in [0, 1, 2, 3]
        assert res["model_name"] == "model_v1"
        seen_ranks.add(res["rank"])
    print(f"Saw ranks: {sorted(seen_ranks)}")
