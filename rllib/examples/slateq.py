"""The SlateQ algorithm for recommendation"""

import argparse

import ray
from ray import tune
from ray.rllib.agents import slateq
from ray.rllib.env.wrappers.recsim_wrapper import env_name as recsim_env_name
from ray.tune.logger import pretty_print


def main():
    ALL_POLICIES = ["random", "greedy", "dqn", "slateq"]
    ALL_LEARNING_METHODS = ["QL", "SARSA", "MYOP"]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--policy",
        type=str,
        default="slateq",
        help=("Select agent policy. Choose from: " + ", ".join(ALL_POLICIES) +
              ". Default value: slateq."),
    )
    parser.add_argument(
        "--learning-method",
        type=str,
        default="QL",
        help=(
            "Learning method of the slateq policy (value ignored for other "
            "policies). Choose from: " + ", ".join(ALL_LEARNING_METHODS) + ". "
            "Default value: QL."),
    )
    parser.add_argument(
        "--use-tune",
        action="store_true",
        help=("Run with Tune so that the results are logged into Tensorboard. "
              "For debugging, it's easier to run without Ray Tune."),
    )
    parser.add_argument("--env-slate-size", type=int, default=2)
    parser.add_argument("--env-random-seed", type=int, default=0)
    parser.add_argument(
        "--num-gpus",
        type=int,
        default=1,
        help="Only used if running with Tune.")
    parser.add_argument(
        "--num-workers",
        type=int,
        default=10,
        help="Only used if running with Tune.")
    args = parser.parse_args()

    slateq_policy = args.policy
    if slateq_policy not in ALL_POLICIES:
        raise ValueError(slateq_policy)

    if args.learning_method not in ALL_LEARNING_METHODS:
        raise ValueError(args.learning_method)

    env_config = {
        "slate_size": args.env_slate_size,
        "seed": args.env_random_seed,
        "convert_to_discrete_action_space": slateq_policy == "dqn",
    }

    ray.init()
    if args.use_tune:
        tune.run(
            "SlateQ",
            stop={"timesteps_total": 2000000},
            name=f"SlateQ/{args.policy}-{args.learning_method}",
            config={
                "env": recsim_env_name,
                "num_gpus": args.num_gpus,
                "num_workers": args.num_workers,
                "slateq_policy": slateq_policy,
                "slateq_policy_learning_method": args.learning_method,
                "env_config": env_config,
            })
    else:
        # directly run using the trainer interface (good for debugging)
        config = slateq.DEFAULT_CONFIG.copy()
        config["num_gpus"] = 0
        config["num_workers"] = 0
        config["slateq_policy"] = slateq_policy
        config["env_config"] = env_config
        trainer = slateq.SlateQTrainer(config=config, env=recsim_env_name)
        for i in range(10):
            result = trainer.train()
            print(pretty_print(result))
    ray.shutdown()


if __name__ == "__main__":
    main()
