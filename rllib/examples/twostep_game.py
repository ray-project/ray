"""The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from gym.spaces import Tuple, Discrete
import numpy as np

import ray
from ray import tune
from ray.tune import register_env, grid_search
from ray.rllib.env.multi_agent_env import MultiAgentEnv

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=50000)
parser.add_argument("--run", type=str, default="PG")


class TwoStepGame(MultiAgentEnv):
    action_space = Discrete(2)

    # Each agent gets a separate [3] obs space, to ensure that they can
    # learn meaningfully different Q values even with a shared Q model.
    observation_space = Discrete(6)

    def __init__(self, env_config):
        self.state = None
        self.agent_1 = 0
        self.agent_2 = 1
        # MADDPG emits action logits instead of actual discrete actions
        self.actions_are_logits = env_config.get("actions_are_logits", False)

    def reset(self):
        self.state = 0
        return {self.agent_1: self.state, self.agent_2: self.state + 3}

    def step(self, action_dict):
        if self.actions_are_logits:
            action_dict = {
                k: np.random.choice([0, 1], p=v)
                for k, v in action_dict.items()
            }

        if self.state == 0:
            action = action_dict[self.agent_1]
            assert action in [0, 1], action
            if action == 0:
                self.state = 1
            else:
                self.state = 2
            global_rew = 0
            done = False
        elif self.state == 1:
            global_rew = 7
            done = True
        else:
            if action_dict[self.agent_1] == 0 and action_dict[self.
                                                              agent_2] == 0:
                global_rew = 0
            elif action_dict[self.agent_1] == 1 and action_dict[self.
                                                                agent_2] == 1:
                global_rew = 8
            else:
                global_rew = 1
            done = True

        rewards = {
            self.agent_1: global_rew / 2.0,
            self.agent_2: global_rew / 2.0
        }
        obs = {self.agent_1: self.state, self.agent_2: self.state + 3}
        dones = {"__all__": done}
        infos = {}
        return obs, rewards, dones, infos


if __name__ == "__main__":
    args = parser.parse_args()

    grouping = {
        "group_1": [0, 1],
    }
    obs_space = Tuple([
        TwoStepGame.observation_space,
        TwoStepGame.observation_space,
    ])
    act_space = Tuple([
        TwoStepGame.action_space,
        TwoStepGame.action_space,
    ])
    register_env(
        "grouped_twostep",
        lambda config: TwoStepGame(config).with_agent_groups(
            grouping, obs_space=obs_space, act_space=act_space))

    if args.run == "contrib/MADDPG":
        obs_space_dict = {
            "agent_1": TwoStepGame.observation_space,
            "agent_2": TwoStepGame.observation_space,
        }
        act_space_dict = {
            "agent_1": TwoStepGame.action_space,
            "agent_2": TwoStepGame.action_space,
        }
        config = {
            "learning_starts": 100,
            "env_config": {
                "actions_are_logits": True,
            },
            "multiagent": {
                "policies": {
                    "pol1": (None, TwoStepGame.observation_space,
                             TwoStepGame.action_space, {
                                 "agent_id": 0,
                             }),
                    "pol2": (None, TwoStepGame.observation_space,
                             TwoStepGame.action_space, {
                                 "agent_id": 1,
                             }),
                },
                "policy_mapping_fn": tune.function(
                    lambda x: "pol1" if x == 0 else "pol2"),
            },
        }
        group = False
    elif args.run == "QMIX":
        config = {
            "sample_batch_size": 4,
            "train_batch_size": 32,
            "exploration_final_eps": 0.0,
            "num_workers": 0,
            "mixer": grid_search([None, "qmix", "vdn"]),
        }
        group = True
    elif args.run == "APEX_QMIX":
        config = {
            "num_gpus": 0,
            "num_workers": 2,
            "optimizer": {
                "num_replay_buffer_shards": 1,
            },
            "min_iter_time_s": 3,
            "buffer_size": 1000,
            "learning_starts": 1000,
            "train_batch_size": 128,
            "sample_batch_size": 32,
            "target_network_update_freq": 500,
            "timesteps_per_iteration": 1000,
        }
        group = True
    else:
        config = {}
        group = False

    ray.init()
    tune.run(
        args.run,
        stop={
            "timesteps_total": args.stop,
        },
        config=dict(config, **{
            "env": "grouped_twostep" if group else TwoStepGame,
        }),
    )
