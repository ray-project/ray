"""Small Ray Tune run (4 trials, 4 actors).

This training run will start 4 Ray Tune Trials, each starting 4 actors.
The cluster comprises 4 nodes.

Test owner: krfricke

Acceptance criteria: Should run through and report final results, as well
as the Ray Tune results table. No trials should error. All trials should
run in parallel.
"""
from collections import Counter
import json
import os
import time

import ray
from ray import tune

from xgboost_ray import RayParams

from release_test_util import train_ray


def train_wrapper(config, ray_params):
    train_ray(
        path="/data/classification.parquet",
        num_workers=None,
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
        "max_depth": tune.randint(1, 9),
    }

    ray.init(address="auto", runtime_env={"working_dir": os.path.dirname(__file__)})

    ray_params = RayParams(
        elastic_training=False,
        max_actor_restarts=2,
        num_actors=4,
        cpus_per_actor=1,
        gpus_per_actor=0,
    )

    start = time.time()
    analysis = tune.run(
        tune.with_parameters(train_wrapper, ray_params=ray_params),
        config=search_space,
        num_samples=4,
        resources_per_trial=ray_params.get_tune_resources(),
    )
    taken = time.time() - start

    result = {
        "time_taken": taken,
        "trial_states": dict(Counter([trial.status for trial in analysis.trials])),
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/tune_small.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("PASSED.")
