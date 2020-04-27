"""The two-step game from QMIX: https://arxiv.org/pdf/1803.11485.pdf

Configurations you can try:
    - normal policy gradients (PG)
    - contrib/MADDPG
    - QMIX
    - APEX_QMIX

See also: centralized_critic.py for centralized critic PPO on this game.
"""

import argparse
from gym.spaces import Tuple, MultiDiscrete, Dict, Discrete
import numpy as np

import ray
from ray import tune
from ray.tune import register_env, grid_search
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.rllib.agents.qmix.qmix_policy import ENV_STATE

parser = argparse.ArgumentParser()
parser.add_argument("--stop", type=int, default=50000)
parser.add_argument("--run", type=str, default="PG")
parser.add_argument("--num-cpus", type=int, default=0)


class TwoStepGame(MultiAgentEnv):
    action_space = Discrete(2)

    def __init__(self, env_config):
        self.state = None
        self.agent_1 = 0
        self.agent_2 = 1
        # MADDPG emits action logits instead of actual discrete actions
        self.actions_are_logits = env_config.get("actions_are_logits", False)
        self.one_hot_state_encoding = env_config.get("one_hot_state_encoding",
                                                     False)
        self.with_state = env_config.get("separate_state_space", False)

        if not self.one_hot_state_encoding:
            self.observation_space = Discrete(6)
            self.with_state = False
        else:
            # Each agent gets the full state (one-hot encoding of which of the
            # three states are active) as input with the receiving agent's
            # ID (1 or 2) concatenated onto the end.
            if self.with_state:
                self.observation_space = Dict({
                    "obs": MultiDiscrete([2, 2, 2, 3]),
                    ENV_STATE: MultiDiscrete([2, 2, 2])
                })
            else:
                self.observation_space = MultiDiscrete([2, 2, 2, 3])

    def reset(self):
        self.state = np.array([1, 0, 0])
        return self._obs()

    def step(self, action_dict):
        if self.actions_are_logits:
            action_dict = {
                k: np.random.choice([0, 1], p=v)
                for k, v in action_dict.items()
            }

        state_index = np.flatnonzero(self.state)
        if state_index == 0:
            action = action_dict[self.agent_1]
            assert action in [0, 1], action
            if action == 0:
                self.state = np.array([0, 1, 0])
            else:
                self.state = np.array([0, 0, 1])
            global_rew = 0
            done = False
        elif state_index == 1:
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
        obs = self._obs()
        dones = {"__all__": done}
        infos = {}
        return obs, rewards, dones, infos

    def _obs(self):
        if self.with_state:
            return {
                self.agent_1: {
                    "obs": self.agent_1_obs(),
                    ENV_STATE: self.state
                },
                self.agent_2: {
                    "obs": self.agent_2_obs(),
                    ENV_STATE: self.state
                }
            }
        else:
            return {
                self.agent_1: self.agent_1_obs(),
                self.agent_2: self.agent_2_obs()
            }

    def agent_1_obs(self):
        if self.one_hot_state_encoding:
            return np.concatenate([self.state, [1]])
        else:
            return np.flatnonzero(self.state)[0]

    def agent_2_obs(self):
        if self.one_hot_state_encoding:
            return np.concatenate([self.state, [2]])
        else:
            return np.flatnonzero(self.state)[0] + 3


if __name__ == "__main__":
    args = parser.parse_args()

    grouping = {
        "group_1": [0, 1],
    }
    obs_space = Tuple([
        Dict({
            "obs": MultiDiscrete([2, 2, 2, 3]),
            ENV_STATE: MultiDiscrete([2, 2, 2])
        }),
        Dict({
            "obs": MultiDiscrete([2, 2, 2, 3]),
            ENV_STATE: MultiDiscrete([2, 2, 2])
        }),
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
            "agent_1": Discrete(6),
            "agent_2": Discrete(6),
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
                    "pol1": (None, Discrete(6), TwoStepGame.action_space, {
                        "agent_id": 0,
                    }),
                    "pol2": (None, Discrete(6), TwoStepGame.action_space, {
                        "agent_id": 1,
                    }),
                },
                "policy_mapping_fn": lambda x: "pol1" if x == 0 else "pol2",
            },
        }
        group = False
    elif args.run == "QMIX":
        config = {
            "rollout_fragment_length": 4,
            "train_batch_size": 32,
            "exploration_fraction": .4,
            "exploration_final_eps": 0.0,
            "num_workers": 0,
            "mixer": grid_search([None, "qmix", "vdn"]),
            "env_config": {
                "separate_state_space": True,
                "one_hot_state_encoding": True
            },
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
            "rollout_fragment_length": 32,
            "target_network_update_freq": 500,
            "timesteps_per_iteration": 1000,
            "env_config": {
                "separate_state_space": True,
                "one_hot_state_encoding": True
            },
        }
        group = True
    else:
        config = {}
        group = False

    ray.init(num_cpus=args.num_cpus or None)
    tune.run(
        args.run,
        stop={
            "timesteps_total": args.stop,
        },
        config=dict(config, **{
            "env": "grouped_twostep" if group else TwoStepGame,
        }),
    )
