"""Using an RLlib-ready RecSim environment and the SlateQ algorithm
for solving recommendation system problems.

This example supports three different RecSim (RLlib-ready) environments,
configured via the --env option:
- "long-term-satisfaction"
- "interest-exploration"
- "interest-evolution"
"""

import argparse
from datetime import datetime

import ray
from ray import tune
from ray.rllib.agents import slateq
from ray.rllib.agents import dqn
from ray.rllib.agents.slateq.slateq import ALL_SLATEQ_STRATEGIES
from ray.rllib.examples.env.recsim_recommender_system_envs import \
    InterestEvolutionRecSimEnv, InterestExplorationRecSimEnv, \
    LongTermSatisfactionRecSimEnv
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import pretty_print

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="SlateQ",
    choices=["SlateQ", "DQN"],
    help=("Select agent policy. Choose from: DQN and SlateQ. "
          "Default value: SlateQ."),
)
parser.add_argument(
    "--env",
    type=str,
    default="interest-evolution",
    choices=[
        "interest-evolution", "interest-exploration", "long-term-satisfaction"
    ],
    help=("Select the RecSim env to use."),
)
parser.add_argument(
    "--slateq-strategy",
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
parser.add_argument("--tune-num-samples", type=int, default=1)

parser.add_argument(
    "--env-num-candidates",
    type=int,
    default=100,
    help="The number of candidates that the agent has to pick "
    "`--env-slate-size` from each timestep. These candidates will be "
    "sampled by the environment's built-in document sampler model.")
parser.add_argument(
    "--env-slate-size",
    type=int,
    default=2,
    help="The size of the slate to recommend (from out of "
    "`--env-num-candidates` sampled docs) each timestep.")
parser.add_argument(
    "--env-dont-resample-documents",
    action="store_true",
    help="Whether to NOT resample `--env-num-candidates` docs "
    "each timestep. If set, the env will only sample `--env-num-candidates`"
    " once at the beginning and the agent always has to pick "
    "`--env-slate-size` docs from this sample.")
parser.add_argument("--env-seed", type=int, default=0)
parser.add_argument("--num-gpus", type=float, default=0)
parser.add_argument("--num-workers", type=int, default=0)

parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.")
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=2000)
parser.add_argument("--stop-reward", type=float, default=180.0)
parser.add_argument("--stop-timesteps", type=int, default=4000000)


def main():
    args = parser.parse_args()
    ray.init(local_mode=args.local_mode)

    env_config = {
        "num_candidates": args.env_num_candidates,
        "resample_documents": not args.env_dont_resample_documents,
        "slate_size": args.env_slate_size,
        "seed": args.env_seed,
        "convert_to_discrete_action_space": args.run == "DQN",
    }

    config = {
        "env": (InterestEvolutionRecSimEnv if args.env == "interest-evolution"
                else InterestExplorationRecSimEnv
                if args.env == "interest-exploration" else
                LongTermSatisfactionRecSimEnv),
        "hiddens": [256, 256],#TEST
        "num_gpus": args.num_gpus,
        "num_workers": args.num_workers,
        "env_config": env_config,
        "lr_choice_model": 0.003,
        "lr_q_model": 0.003,
        "rollout_fragment_length": 4,
        "exploration_config": {
            "epsilon_timesteps": 10000,#500000,#TODO
        },
        "target_network_update_freq": 800,#5000,TODO
    }

    if args.use_tune:
        stop = {
            "training_iteration": args.stop_iters,
            "timesteps_total": args.stop_timesteps,
            "episode_reward_mean": args.stop_reward,
        }

        time_signature = datetime.now().strftime("%Y-%m-%d_%H_%M_%S")
        name = f"SlateQ/{args.run}-seed{args.env_seed}-{time_signature}"
        if args.run == "SlateQ":
            config.update({
                "slateq_strategy": args.slateq_strategy,
            })
        results = tune.run(
            args.run,
            stop=stop,
            name=name,
            config=config,
            num_samples=args.tune_num_samples,
            verbose=2)

        if args.as_test:
            check_learning_achieved(results, args.stop_reward)

    else:
        # Directly run using the trainer interface (good for debugging).
        if args.run == "DQN":
            trainer = dqn.DQNTrainer(config=config)
        else:
            config.update({
                "slateq_strategy": args.slateq_strategy,
            })
            trainer = slateq.SlateQTrainer(config=config)
        for i in range(10):
            result = trainer.train()
            print(pretty_print(result))
    ray.shutdown()


if __name__ == "__main__":
    main()
