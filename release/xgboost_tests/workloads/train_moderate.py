"""Moderate cluster training

This training run will start 32 workers on 32 nodes (including head node).

Test owner: krfricke

Acceptance criteria: Should run through and report final results.
"""
import ray
from xgboost_ray import RayParams

from _train import train_ray

if __name__ == "__main__":
    ray.init(address="auto")

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=32,
        cpus_per_actor=4,
        gpus_per_actor=0)

    train_ray(
        path="/data/classification.parquet",
        num_workers=32,
        num_boost_rounds=100,
        num_files=128,
        regression=False,
        use_gpu=False,
        ray_params=ray_params,
        xgboost_params=None,
    )
