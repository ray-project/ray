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
from scipy.stats import sem

import ray
from ray import air, tune
from ray.rllib.algorithms import slateq
from ray.rllib.algorithms import dqn
from ray.rllib.examples.env.recommender_system_envs_with_recsim import (
    InterestEvolutionRecSimEnv,
    InterestExplorationRecSimEnv,
    LongTermSatisfactionRecSimEnv,
)
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.logger import pretty_print

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run",
    type=str,
    default="SlateQ",
    choices=["SlateQ", "DQN"],
    help=("Select agent policy. Choose from: DQN and SlateQ. Default value: SlateQ."),
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "tfe", "torch"],
    default="tf",
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
parser.add_argument(
    "--use-tune",
    action="store_true",
    help=(
        "Run with Tune so that the results are logged into Tensorboard. "
        "For debugging, it's easier to run without Ray Tune."
    ),
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
parser.add_argument("--env-seed", type=int, default=0)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument("--num-gpus", type=float, default=0)
parser.add_argument("--num-workers", type=int, default=0)

parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-reward", type=float, default=160.0)
parser.add_argument("--stop-timesteps", type=int, default=150000)


def main():
    args = parser.parse_args()
    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    env_config = {
        "num_candidates": args.env_num_candidates,
        "resample_documents": not args.env_dont_resample_documents,
        "slate_size": args.env_slate_size,
        "seed": args.env_seed,
        "convert_to_discrete_action_space": args.run == "DQN",
    }

    config = {
        "env": (
            InterestEvolutionRecSimEnv
            if args.env == "interest-evolution"
            else InterestExplorationRecSimEnv
            if args.env == "interest-exploration"
            else LongTermSatisfactionRecSimEnv
        ),
        "framework": args.framework,
        "num_gpus": args.num_gpus,
        "num_workers": args.num_workers,
        "env_config": env_config,
        "num_steps_sampled_before_learning_starts": args.num_steps_sampled_before_learning_starts,  # noqa E501
    }

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
            _, r, d, _ = env.step(action)
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

    if args.use_tune:
        stop = {
            "training_iteration": args.stop_iters,
            "timesteps_total": args.stop_timesteps,
            "episode_reward_mean": args.stop_reward,
        }

        results = tune.Tuner(
            args.run,
            run_config=air.RunConfig(
                stop=stop,
                verbose=2,
            ),
            param_space=config,
            tune_config=tune.TuneConfig(
                num_samples=args.tune_num_samples,
            ),
        ).fit()

        if args.as_test:
            check_learning_achieved(results, args.stop_reward)

    else:
        # Directly run using the trainer interface (good for debugging).
        if args.run == "DQN":
            trainer = dqn.DQN(config=config)
        else:
            trainer = slateq.SlateQ(config=config)
        for i in range(10):
            result = trainer.train()
            print(pretty_print(result))
    ray.shutdown()


if __name__ == "__main__":
    main()
