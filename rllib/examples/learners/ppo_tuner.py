import argparse

import ray
from ray import air, tune
from ray.air.constants import TRAINING_ITERATION
from ray.rllib.algorithms.ppo import PPOConfig

LEARNER_CONFIG = {
    "remote-cpu": {"num_learners": 1},
    "remote-gpu": {"num_learners": 1, "num_gpus_per_learner": 1},
    "multi-gpu-ddp": {
        "num_learners": 2,
        "num_gpus_per_learner": 1,
    },
    "local-cpu": {},
    "local-gpu": {"num_gpus_per_learner": 1},
}


def _parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=str,
        default="local-cpu",
    )

    parser.add_argument(
        "--framework",
        choices=["tf2", "torch"],  # tf will be deprecated with the new Learner stack
        default="torch",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    ray.init()

    config = (
        PPOConfig()
        .framework(args.framework)
        .environment("CartPole-v1")
        .learners(**LEARNER_CONFIG[args.config])
    )

    print("Testing with learner config: ", LEARNER_CONFIG[args.config])
    print("Testing with framework: ", args.framework)
    print("-" * 80)
    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={TRAINING_ITERATION: 1},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    tuner.fit()
