"""The SlateQ algorithm for recommendation"""

import argparse
from datetime import datetime

import ray
from ray import tune
from ray.rllib.agents import slateq
from ray.rllib.agents import dqn
from ray.rllib.agents.slateq.slateq import ALL_SLATEQ_STRATEGIES
from ray.rllib.env.wrappers.recsim_wrapper import env_name as recsim_env_name
from ray.tune.logger import pretty_print


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--agent",
        type=str,
        default="SlateQ",
        help=("Select agent policy. Choose from: DQN and SlateQ. "
              "Default value: SlateQ."),
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="QL",
        help=("Strategy for the SlateQ agent. Choose from: " +
              ", ".join(ALL_SLATEQ_STRATEGIES) + ". "
              "Default value: QL. Ignored when using Tune."),
    )
    parser.add_argument(
        "--use-tune",
        action="store_true",
        help=("Run with Tune so that the results are logged into Tensorboard. "
              "For debugging, it's easier to run without Ray Tune."),
    )
    parser.add_argument("--tune-num-samples", type=int, default=10)
    parser.add_argument("--env-slate-size", type=int, default=2)
    parser.add_argument("--env-seed", type=int, default=0)
    parser.add_argument(
        "--num-gpus",
        type=float,
        default=0.,
        help="Only used if running with Tune.")
    parser.add_argument(
        "--num-workers",
        type=int,
        default=0,
        help="Only used if running with Tune.")
    args = parser.parse_args()

    if args.agent not in ["DQN", "SlateQ"]:
        raise ValueError(args.agent)

    env_config = {
        "slate_size": args.env_slate_size,
        "seed": args.env_seed,
        "convert_to_discrete_action_space": args.agent == "DQN",
    }

    ray.init()
    if args.use_tune:
        time_signature = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        name = f"SlateQ/{args.agent}-seed{args.env_seed}-{time_signature}"
        if args.agent == "DQN":
            tune.run(
                "DQN",
                stop={"timesteps_total": 4000000},
                name=name,
                config={
                    "env": recsim_env_name,
                    "num_gpus": args.num_gpus,
                    "num_workers": args.num_workers,
                    "env_config": env_config,
                },
                num_samples=args.tune_num_samples,
                verbose=1)
        else:
            tune.run(
                "SlateQ",
                stop={"timesteps_total": 4000000},
                name=name,
                config={
                    "env": recsim_env_name,
                    "num_gpus": args.num_gpus,
                    "num_workers": args.num_workers,
                    "slateq_strategy": tune.grid_search(ALL_SLATEQ_STRATEGIES),
                    "env_config": env_config,
                },
                num_samples=args.tune_num_samples,
                verbose=1)
    else:
        # directly run using the trainer interface (good for debugging)
        if args.agent == "DQN":
            config = dqn.DEFAULT_CONFIG.copy()
            config["num_gpus"] = 0
            config["num_workers"] = 0
            config["env_config"] = env_config
            trainer = dqn.DQNTrainer(config=config, env=recsim_env_name)
        else:
            config = slateq.DEFAULT_CONFIG.copy()
            config["num_gpus"] = 0
            config["num_workers"] = 0
            config["slateq_strategy"] = args.strategy
            config["env_config"] = env_config
            trainer = slateq.SlateQTrainer(config=config, env=recsim_env_name)
        for i in range(10):
            result = trainer.train()
            print(pretty_print(result))
    ray.shutdown()


if __name__ == "__main__":
    main()
