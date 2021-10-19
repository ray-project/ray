from ray.util.client import ray

from ray.tune import tune

ray.connect("localhost:50051")

tune.run("PG", config={"env": "CartPole-v0"})
