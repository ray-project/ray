# The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf

import argparse
import logging

from gymnasium.spaces import Dict, Discrete, MultiDiscrete, Tuple
from rllib_maddpg.maddpg import MADDPG, MADDPGConfig

import ray
from ray import air, tune
from ray.rllib.env.multi_agent_env import ENV_STATE
from ray.rllib.examples.env.two_step_game import TwoStepGame
from ray.rllib.policy.policy import PolicySpec
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
    "--stop-timesteps", type=int, default=20000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=7.2, help="Reward at which we stop training."
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
        MADDPGConfig()
        .environment(TwoStepGame)
        .framework("torch")
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources()
    )

    obs_space = Discrete(6)
    act_space = TwoStepGame.action_space
    (
        config.framework("tf")
        .environment(env_config={"actions_are_logits": True})
        .training(num_steps_sampled_before_learning_starts=200)
        .multi_agent(
            policies={
                "pol1": PolicySpec(
                    observation_space=obs_space,
                    action_space=act_space,
                    config=config.overrides(agent_id=0),
                ),
                "pol2": PolicySpec(
                    observation_space=obs_space,
                    action_space=act_space,
                    config=config.overrides(agent_id=1),
                ),
            },
            policy_mapping_fn=lambda agent_id, episode, worker, **kwargs: "pol2"
            if agent_id
            else "pol1",
        )
    )

    stop = {
        "episode_reward_mean": args.stop_reward,
        "timesteps_total": args.stop_timesteps,
    }

    results = tune.Tuner(
        MADDPG,
        run_config=air.RunConfig(stop=stop, verbose=2),
        param_space=config,
    ).fit()

    if args.run_as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
