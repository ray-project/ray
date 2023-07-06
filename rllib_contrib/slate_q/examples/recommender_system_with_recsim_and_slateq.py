"""Using an RLlib-ready RecSim environment and the SlateQ algorithm
for solving recommendation system problems.

This example supports three different RecSim (RLlib-ready) environments,
configured via the --env option:
- "long-term-satisfaction"
- "interest-exploration"
- "interest-evolution"
"""

import argparse

import numpy as np
from rllib_slate_q.slate_q import SlateQ, SlateQConfig
from scipy.stats import sem

import ray
from ray import air, tune
from ray.rllib.examples.env.recommender_system_envs_with_recsim import (
    InterestEvolutionRecSimEnv,
    InterestExplorationRecSimEnv,
    LongTermSatisfactionRecSimEnv,
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--env",
    type=str,
    default="interest-evolution",
    choices=["interest-evolution", "interest-exploration", "long-term-satisfaction"],
    help=("Select the RecSim env to use."),
)

parser.add_argument(
    "--random-test-episodes",
    type=int,
    default=0,
    help="The number of test episodes to run with a random agent to figure out "
    "up front what the random baseline reward is.",
)

parser.add_argument("--tune-num-samples", type=int, default=1)

parser.add_argument(
    "--env-num-candidates",
    type=int,
    default=100,
    help="The number of candidates that the agent has to pick "
    "`--env-slate-size` from each timestep. These candidates will be "
    "sampled by the environment's built-in document sampler model.",
)

parser.add_argument(
    "--num-steps-sampled-before-learning_starts",
    type=int,
    default=20000,
    help="Number of timesteps to collect from rollout workers before we start "
    "sampling from replay buffers for learning..",
)

parser.add_argument(
    "--env-slate-size",
    type=int,
    default=2,
    help="The size of the slate to recommend (from out of "
    "`--env-num-candidates` sampled docs) each timestep.",
)
parser.add_argument(
    "--env-dont-resample-documents",
    action="store_true",
    help="Whether to NOT resample `--env-num-candidates` docs "
    "each timestep. If set, the env will only sample `--env-num-candidates`"
    " once at the beginning and the agent always has to pick "
    "`--env-slate-size` docs from this sample.",
)

parser.add_argument("--run-as-test", action="store_true")


def main():
    args = parser.parse_args()
    ray.init()

    env_config = {
        "num_candidates": args.env_num_candidates,
        "resample_documents": not args.env_dont_resample_documents,
        "slate_size": args.env_slate_size,
        "seed": 0,
        "convert_to_discrete_action_space": False,
    }

    config = (
        SlateQConfig()
        .environment(
            InterestEvolutionRecSimEnv
            if args.env == "interest-evolution"
            else InterestExplorationRecSimEnv
            if args.env == "interest-exploration"
            else LongTermSatisfactionRecSimEnv,
            env_config=env_config,
        )
        .framework(args.framework)
        .rollouts(num_rollout_workers=7)
        .resources()
    )

    config.num_steps_sampled_before_learning_starts = (
        args.num_steps_sampled_before_learning_starts
    )

    # Perform a test run on the env with a random agent to see, what
    # the random baseline reward is.
    if args.random_test_episodes:
        print(
            f"Running {args.random_test_episodes} episodes to get a random "
            "agent's baseline reward ..."
        )
        env = config["env"](config=env_config)
        env.reset()
        num_episodes = 0
        episode_rewards = []
        episode_reward = 0.0
        while num_episodes < args.random_test_episodes:
            action = env.action_space.sample()
            _, r, d, _, _ = env.step(action)
            episode_reward += r
            if d:
                num_episodes += 1
                episode_rewards.append(episode_reward)
                episode_reward = 0.0
                env.reset()
        print(
            f"Ran {args.random_test_episodes} episodes with a random agent "
            "reaching a mean episode return of "
            f"{np.mean(episode_rewards)}+/-{sem(episode_rewards)}."
        )

    if args.run_as_test:
        stop = {"training_iteration": 1}
    else:
        stop = {
            "training_iteration": 200,
            "timesteps_total": 150000,
            "episode_reward_mean": 160,
        }

    tune.Tuner(
        SlateQ,
        run_config=air.RunConfig(
            stop=stop,
        ),
        param_space=config,
    ).fit()


if __name__ == "__main__":
    main()
