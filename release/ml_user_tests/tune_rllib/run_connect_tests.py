"""Connect tests for Tune & RLlib.

Runs a couple of hard learning tests using Anyscale connect.
"""

import json
import logging
import os
from pprint import pformat
import time

import ray

from ray import air, tune
from ray.rllib.algorithms.appo import APPOConfig
from ray.tune import CLIReporter

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("tune_framework")


def run(smoke_test=False, storage_path: str = None):
    stop = {"training_iteration": 1 if smoke_test else 50}
    num_workers = 1 if smoke_test else 20
    num_gpus = 0 if smoke_test else 1

    config = (
        APPOConfig()
        .environment("ALE/Pong-v5", clip_rewards=True)
        .framework(tune.grid_search(["tf", "torch"]))
        .rollouts(
            rollout_fragment_length=50,
            num_rollout_workers=num_workers,
            num_envs_per_worker=1,
        )
        .training(
            train_batch_size=750,
            num_sgd_iter=2,
            vf_loss_coeff=1.0,
            clip_param=0.3,
            grad_clip=10,
            vtrace=True,
            use_kl_loss=False,
        )
        .resources(num_gpus=num_gpus)
    )

    logger.info("Configuration: \n %s", pformat(config))

    # Run the experiment.
    return tune.Tuner(
        "APPO",
        param_space=config,
        run_config=air.RunConfig(
            stop=stop,
            verbose=1,
            progress_reporter=CLIReporter(
                metric_columns={
                    "training_iteration": "iter",
                    "time_total_s": "time_total_s",
                    "timesteps_total": "ts",
                    "snapshots": "snapshots",
                    "episodes_this_iter": "train_episodes",
                    "episode_reward_mean": "reward_mean",
                },
                sort_by_metric=True,
                max_report_frequency=30,
            ),
            storage_path=storage_path,
        ),
        tune_config=tune.TuneConfig(
            num_samples=1,
        ),
    ).fit()


if __name__ == "__main__":
    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "rllib_connect_tests")
    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    start_time = time.time()
    results = run(storage_path="/mnt/cluster_storage")
    exp_analysis = results._experiment_analysis
    end_time = time.time()

    result = {
        "time_taken": end_time - start_time,
        "trial_states": {t.config["framework"]: t.status for t in exp_analysis.trials},
    }

    test_output_json = os.environ.get("TEST_OUTPUT_JSON", "/tmp/release_test_out.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Ok.")
