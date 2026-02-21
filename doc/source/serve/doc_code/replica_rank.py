# __replica_rank_start__
from ray import serve


@serve.deployment(num_replicas=4)
class ModelShard:
    def __call__(self):
        context = serve.get_replica_context()
        return {
            "rank": context.rank.rank,  # Access the integer rank value
            "world_size": context.world_size,
        }


app = ModelShard.bind()
# __replica_rank_end__

# __reconfigure_rank_start__
from typing import Any
from ray import serve
from ray.serve.schema import ReplicaRank


@serve.deployment(num_replicas=4, user_config={"name": "model_v1"})
class RankAwareModel:
    def __init__(self):
        context = serve.get_replica_context()
        self.rank = context.rank.rank  # Extract integer rank value
        self.world_size = context.world_size
        self.model_name = None
        print(f"Replica rank: {self.rank}/{self.world_size}")

    async def reconfigure(self, user_config: Any, rank: ReplicaRank):
        """Called when user_config or rank changes."""
        self.rank = rank.rank  # Extract integer rank value from ReplicaRank object
        self.world_size = serve.get_replica_context().world_size
        self.model_name = user_config.get("name")
        print(f"Reconfigured: rank={self.rank}, model={self.model_name}")

    def __call__(self):
        return {"rank": self.rank, "model_name": self.model_name}


app2 = RankAwareModel.bind()
# __reconfigure_rank_end__

if __name__ == "__main__":
    # __replica_rank_start_run_main__
    h = serve.run(app)
    # Test that we can get rank information from replicas
    seen_ranks = set()
    for _ in range(20):
        res = h.remote().result()
        print(f"Output from __call__: {res}")
        assert res["rank"] in [0, 1, 2, 3]
        assert res["world_size"] == 4
        seen_ranks.add(res["rank"])

    # Verify we hit all replicas
    print(f"Saw ranks: {sorted(seen_ranks)}")

    # Output from __call__: {'rank': 2, 'world_size': 4}
    # Output from __call__: {'rank': 1, 'world_size': 4}
    # Output from __call__: {'rank': 3, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 3, 'world_size': 4}
    # Output from __call__: {'rank': 1, 'world_size': 4}
    # Output from __call__: {'rank': 1, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 1, 'world_size': 4}
    # Output from __call__: {'rank': 3, 'world_size': 4}
    # Output from __call__: {'rank': 2, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Output from __call__: {'rank': 2, 'world_size': 4}
    # Output from __call__: {'rank': 1, 'world_size': 4}
    # Output from __call__: {'rank': 3, 'world_size': 4}
    # Output from __call__: {'rank': 0, 'world_size': 4}
    # Saw ranks: [0, 1, 2, 3]

    # __replica_rank_end_run_main__

    # __reconfigure_rank_start_run_main__
    h = serve.run(app2)
    for _ in range(20):
        res = h.remote().result()
        assert res["rank"] in [0, 1, 2, 3]
        assert res["model_name"] == "model_v1"
        seen_ranks.add(res["rank"])

    # (ServeReplica:default:RankAwareModel pid=1231505) Replica rank: 0/4
    # (ServeReplica:default:RankAwareModel pid=1231505) Reconfigured: rank=0, model=model_v1
    # (ServeReplica:default:RankAwareModel pid=1231504) Replica rank: 1/4
    # (ServeReplica:default:RankAwareModel pid=1231504) Reconfigured: rank=1, model=model_v1
    # (ServeReplica:default:RankAwareModel pid=1231502) Replica rank: 3/4
    # (ServeReplica:default:RankAwareModel pid=1231502) Reconfigured: rank=3, model=model_v1
    # (ServeReplica:default:RankAwareModel pid=1231503) Replica rank: 2/4
    # (ServeReplica:default:RankAwareModel pid=1231503) Reconfigured: rank=2, model=model_v1
    # __reconfigure_rank_end_run_main__
