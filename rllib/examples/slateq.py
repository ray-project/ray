"""The SlateQ algorithm for recommendation"""

import argparse

import ray
from ray import tune
from ray.rllib.agents import slateq
from ray.rllib.env.wrappers.recsim_wrapper import env_name as recsim_env_name
from ray.tune.logger import pretty_print


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--policy", type=str, default="slateq")
    parser.add_argument("--num-gpus", type=int, default=1)
    parser.add_argument("--use-tune", action="store_true")
    parser.add_argument("--slate-size", type=int, default=2)
    parser.add_argument("--num-workers", type=int, default=10)
    args = parser.parse_args()

    slateq_policy = args.policy
    ALL_POLICIES = ["random", "greedy", "dqn", "slateq"]
    if slateq_policy not in ALL_POLICIES:
        raise ValueError(slateq_policy)

    ray.init()
    if args.use_tune:
        tune.run(
            "SlateQ",
            stop={"timesteps_total": 2000000},
            config={
                "env": recsim_env_name,
                "num_gpus": args.num_gpus,
                "num_workers": args.num_workers,
                "slateq_policy": slateq_policy,
                "env_config": {
                    "slate_size": args.slate_size,
                    "convert_to_discrete_action_space": slateq_policy == "dqn"
                }
            })
    else:
        # directly run using the trainer interface (good for debugging)
        config = slateq.DEFAULT_CONFIG.copy()
        config["num_gpus"] = 0
        config["num_workers"] = 0
        config["slateq_policy"] = slateq_policy
        config["env_config"] = {
            "slate_size": args.slate_size,
            "convert_to_discrete_action_space": slateq_policy == "dqn"
        }
        trainer = slateq.SlateQTrainer(config=config, env=recsim_env_name)
        for i in range(10):
            result = trainer.train()
            print(pretty_print(result))
    ray.shutdown()


if __name__ == "__main__":
    main()
