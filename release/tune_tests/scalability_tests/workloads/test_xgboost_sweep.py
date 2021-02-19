"""Large-scale XGBoost parameter sweep

In this run, we will start 32 trials of 32 actors each running distributed
XGBoost training. This test is more about making sure that the run succeeds
than about total runtime. However, it is expected that this is faster than
1 hour.

We fix the max_depth to 4 and the number of boosting rounds to 100. The
fastest observed training time for 32 actors (1 CPU each) was about 2000
seconds. We allow up to 10 minutes of slack, so aim for 2600 seconds total
tuning time.

Cluster: cluster_16x64_data.yaml

Test owner: krfricke

Acceptance criteria: Should run faster than 2600 seconds. Should run without
errors.
"""
import os
import time

import ray
from ray import tune

from xgboost_ray import train, RayParams, RayDMatrix


def xgboost_train(config, num_actors=128, num_boost_round=200):
    train_set = RayDMatrix(
        os.path.expanduser("~/data/train.parquet"), "labels")
    test_set = RayDMatrix(os.path.expanduser("~/data/test.parquet"), "labels")

    evals_result = {}

    bst = train(
        params=config,
        dtrain=train_set,
        evals=[(test_set, "eval")],
        evals_result=evals_result,
        ray_params=RayParams(
            max_actor_restarts=1,
            gpus_per_actor=0,
            cpus_per_actor=1,
            num_actors=num_actors),
        verbose_eval=False,
        num_boost_round=num_boost_round)

    model_path = "tuned.xgb"
    bst.save_model(model_path)
    print("Final validation error: {:.4f}".format(
        evals_result["eval"]["error"][-1]))


def main():
    name = "large xgboost sweep"

    ray.init(address="auto")

    num_samples = 32
    num_actors_per_sample = 32

    max_runtime = 2600

    config = {
        "tree_method": "approx",
        "objective": "binary:logistic",
        "eval_metric": ["logloss", "error"],
        "eta": tune.loguniform(1e-4, 1e-1),
        "subsample": tune.uniform(0.5, 1.0),
        "max_depth": 4
    }

    start_time = time.monotonic()
    tune.run(
        tune.with_parameters(
            xgboost_train,
            num_actors=num_actors_per_sample,
            num_boost_round=100),
        config=config,
        num_samples=num_samples)
    time_taken = time.monotonic() - start_time

    assert time_taken < max_runtime, \
        f"The {name} test took {time_taken:.2f} seconds, but should not " \
        f"have exceeded {max_runtime:.2f} seconds. Test failed. \n\n" \
        f"--- FAILED: {name.upper()} ::: " \
        f"{time_taken:.2f} > {max_runtime:.2f} ---"

    print(f"The {name} test took {time_taken:.2f} seconds, which "
          f"is below the budget of {max_runtime:.2f} seconds. "
          f"Test successful. \n\n"
          f"--- PASSED: {name.upper()} ::: "
          f"{time_taken:.2f} <= {max_runtime:.2f} ---")


if __name__ == "__main__":
    main()
