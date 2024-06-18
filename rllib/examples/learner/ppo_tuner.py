import argparse

import ray
from ray import air, tune
from ray.rllib.algorithms.ppo import PPOConfig

RESOURCE_CONFIG = {
    "remote-cpu": {"num_learner_workers": 1},
    "remote-acc": {"num_learner_workers": 1, "num_accs_per_learner_worker": 1},
    "multi-acc-ddp": {
        "num_learner_workers": 2,
        "num_accs_per_learner_worker": 1,
    },
    "local-cpu": {},
    "local-acc": {"num_accs_per_learner_worker": 1},
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
        .resources(**RESOURCE_CONFIG[args.config])
    )

    print("Testing with resource config: ", RESOURCE_CONFIG[args.config])
    print("Testing with framework: ", args.framework)
    print("-" * 80)
    tuner = tune.Tuner(
        "PPO",
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"training_iteration": 1},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    tuner.fit()
