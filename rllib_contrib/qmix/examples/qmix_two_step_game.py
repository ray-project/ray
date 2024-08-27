# The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf

import argparse
import logging

from gymnasium.spaces import Dict, MultiDiscrete, Tuple
from rllib_qmix.qmix import QMix, QMixConfig

import ray
from ray import air, tune
from ray.rllib.env.multi_agent_env import ENV_STATE
from ray.rllib.examples.env.two_step_game import TwoStepGame
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune import register_env

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--mixer",
    type=str,
    default="qmix",
    choices=["qmix", "vdn", "none"],
    help="The mixer model to use.",
)
parser.add_argument(
    "--run-as-test",
    action="store_true",
)

parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=8.0, help="Reward at which we stop training."
)


if __name__ == "__main__":
    args = parser.parse_args()

    ray.init()

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
        QMixConfig()
        .environment(TwoStepGame)
        .framework("torch")
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources()
    )

    (
        config.framework("torch")
        .training(mixer=args.mixer, train_batch_size=32)
        .rollouts(num_rollout_workers=0, rollout_fragment_length=4)
        .exploration(
            exploration_config={
                "final_epsilon": 0.0,
            }
        )
        .environment(
            env="grouped_twostep",
            env_config={
                "separate_state_space": True,
                "one_hot_state_encoding": True,
            },
        )
    )

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.Tuner(
        QMix,
        run_config=air.RunConfig(stop=stop),
        param_space=config,
    ).fit()

    if args.run_as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
