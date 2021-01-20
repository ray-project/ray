"""Small Ray Tune run (4 trials, 4 actors).

This training run will start 4 Ray Tune Trials, each starting 4 actors.
The cluster comprises 4 nodes.

Test owner: krfricke

Acceptance criteria: Should run through and report final results, as well
as the Ray Tune results table. No trials should error. All trials should
run in parallel.
"""
import ray
from ray import tune

from xgboost_ray import RayParams

from _train import train_ray


def train_wrapper(config):
    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=1,
        gpus_per_actor=0)

    train_ray(
        path="/data/classification.parquet",
        num_workers=4,
        num_boost_rounds=100,
        num_files=25,
        regression=False,
        use_gpu=False,
        ray_params=ray_params,
        xgboost_params=config,
    )


if __name__ == "__main__":
    search_space = {
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": tune.randint(1, 9)
    }

    ray.init(address="auto")

    analysis = tune.run(
        train_wrapper,
        config=search_space,
        num_samples=4,
        resources_per_trial={
            "cpu": 1,
            "extra_cpu": 3
        })
