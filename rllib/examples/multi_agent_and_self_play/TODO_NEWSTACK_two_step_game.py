"""The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf

Configurations you can try:
    - normal policy gradients (PG)
    - MADDPG
    - QMIX

See also: centralized_critic.py for centralized critic PPO on this game.
"""

import argparse
from gymnasium.spaces import Dict, Tuple, MultiDiscrete
import logging
import os

import ray
from ray import air, tune
from ray.tune import register_env
from ray.rllib.env.multi_agent_env import ENV_STATE
from ray.rllib.examples.env.two_step_game import TwoStepGame
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import get_trainable_cls

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--run", type=str, default="PPO", help="The RLlib-registered algorithm to use."
)
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="torch",
    help="The DL framework specifier.",
)
parser.add_argument("--num-cpus", type=int, default=0)
parser.add_argument(
    "--mixer",
    type=str,
    default="qmix",
    choices=["qmix", "vdn", "none"],
    help="The mixer model to use.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=200, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=70000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=8.0, help="Reward at which we stop training."
)
parser.add_argument(
    "--local-mode",
    action="store_true",
    help="Init Ray in local mode for easier debugging.",
)

if __name__ == "__main__":
    args = parser.parse_args()

    ray.init(num_cpus=args.num_cpus or None, local_mode=args.local_mode)

    grouping = {
        "group_1": [0, 1],
    }
    obs_space = Tuple(
        [
            Dict(
                {
                    "obs": MultiDiscrete([2, 2, 2, 3]),
                    ENV_STATE: MultiDiscrete([2, 2, 2]),
                }
            ),
            Dict(
                {
                    "obs": MultiDiscrete([2, 2, 2, 3]),
                    ENV_STATE: MultiDiscrete([2, 2, 2]),
                }
            ),
        ]
    )
    act_space = Tuple(
        [
            TwoStepGame.action_space,
            TwoStepGame.action_space,
        ]
    )
    register_env(
        "grouped_twostep",
        lambda config: TwoStepGame(config).with_agent_groups(
            grouping, obs_space=obs_space, act_space=act_space
        ),
    )

    config = (
        get_trainable_cls(args.run)
        .get_default_config()
        .environment(TwoStepGame)
        .framework(args.framework)
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
        "training_iteration": args.stop_iters,
    }

    results = tune.Tuner(
        args.run,
        run_config=air.RunConfig(stop=stop, verbose=2),
        param_space=config,
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
