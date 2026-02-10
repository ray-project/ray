from ray.tune import tune
from ray.util.client import ray

ray.connect("localhost:50051")

tune.run("PG", config={"env": "CartPole-v0"})
