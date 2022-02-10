#!/usr/bin/env python3
""" Benchmarking TF against PyTorch on an example task using Ray Tune.
"""

import logging
from pprint import pformat

import ray
from ray import tune
from ray.tune import CLIReporter

logging.basicConfig(level=logging.WARN)
logger = logging.getLogger("tune_framework")


def run(smoke_test=False):
    stop = {"training_iteration": 1 if smoke_test else 50}
    num_workers = 1 if smoke_test else 20
    num_gpus = 0 if smoke_test else 1

    config = {
        "env": "PongNoFrameskip-v4",
        "framework": tune.grid_search(["tf", "torch"]),
        "num_gpus": num_gpus,
        "rollout_fragment_length": 50,
        "train_batch_size": 750,
        "num_workers": num_workers,
        "num_envs_per_worker": 1,
        "clip_rewards": True,
        "num_sgd_iter": 2,
        "vf_loss_coeff": 1.0,
        "clip_param": 0.3,
        "grad_clip": 10,
        "vtrace": True,
        "use_kl_loss": False,
    }
    logger.info("Configuration: \n %s", pformat(config))

    # Run the experiment.
    # TODO(jungong) : maybe add checkpointing.
    return tune.run(
        "APPO",
        config=config,
        stop=stop,
        verbose=1,
        num_samples=1,
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
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Tune+RLlib Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )

    args = parser.parse_args()

    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init()

    run(smoke_test=args.smoke_test)
    ray.shutdown()
