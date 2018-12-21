from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from gym.spaces import Discrete, Box, Dict, Tuple
import os
import sys
import tensorflow as tf
import tensorflow.contrib.slim as slim
import yaml

import ray
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.tune.registry import register_env
from ray.rllib.models import Model, ModelCatalog
from ray.rllib.models.misc import normc_initializer
from ray.rllib.agents.qmix import QMixAgent
from ray.rllib.agents.pg import PGAgent
from ray.rllib.agents.ppo import PPOAgent
from ray.tune.logger import pretty_print


class ParametricActionsModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        action_mask = input_dict["obs"]["action_mask"]
        if num_outputs != action_mask.shape[1].value:
            raise ValueError(
                "This model assumes num outputs is equal to max avail actions",
                num_outputs, action_mask)

        # Standard FC net component.
        last_layer = input_dict["obs"]["real_obs"]
        hiddens = [256, 256]
        for i, size in enumerate(hiddens):
            label = "fc{}".format(i)
            last_layer = slim.fully_connected(
                last_layer,
                size,
                weights_initializer=normc_initializer(1.0),
                activation_fn=tf.nn.tanh,
                scope=label)
        action_logits = slim.fully_connected(
            last_layer,
            num_outputs,
            weights_initializer=normc_initializer(0.01),
            activation_fn=None,
            scope="fc_out")

        # Mask out invalid actions (use tf.float32.min for stability)
        inf_mask = tf.maximum(tf.log(action_mask), tf.float32.min)
        masked_logits = inf_mask + action_logits

        return masked_logits, last_layer


class SC2MultiAgentEnv(MultiAgentEnv):
    """RLlib Wrapper around StarCraft"""

    def __init__(self, override_cfg):
        PYMARL_PATH = override_cfg.pop("pymarl_path")
        os.environ["SC2PATH"] = os.path.join(PYMARL_PATH,
                                             "3rdparty/StarCraftII")
        sys.path.append(os.path.join(PYMARL_PATH, "src"))
        from envs.starcraft2 import StarCraft2Env
        curpath = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(curpath, "sc2.yaml")) as f:
            pymarl_args = yaml.load(f)
            pymarl_args.update(override_cfg)
            # HACK
            pymarl_args["env_args"].setdefault("seed", 0)

        self._starcraft_env = StarCraft2Env(**pymarl_args)
        obs_size = self._starcraft_env.get_obs_size()
        num_actions = self._starcraft_env.get_total_actions()
        self.observation_space = Dict({
            "action_mask": Box(0, 1, shape=(num_actions, )),
            "real_obs": Box(-1, 1, shape=(obs_size, ))
        })
        self.action_space = Discrete(self._starcraft_env.get_total_actions())

    def reset(self):
        obs_list, state_list = self._starcraft_env.reset()
        return_obs = {}
        for i, obs in enumerate(obs_list):
            return_obs[i] = {
                "action_mask": self._starcraft_env.get_avail_agent_actions(i),
                "real_obs": obs
            }
        return return_obs

    def step(self, action_dict):
        # TODO(rliaw): Check to handle missing agents, if any
        actions = [action_dict[k] for k in sorted(action_dict)]
        rew, done, info = self._starcraft_env.step(actions)
        obs_list = self._starcraft_env.get_obs()
        return_obs = {}
        for i, obs in enumerate(obs_list):
            return_obs[i] = {
                "action_mask": self._starcraft_env.get_avail_agent_actions(i),
                "real_obs": obs
            }
        rews = {i: rew / len(obs_list) for i in range(len(obs_list))}
        dones = {i: done for i in range(len(obs_list))}
        dones["__all__"] = done
        infos = {i: info for i in range(len(obs_list))}
        return return_obs, rews, dones, infos


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-iters", type=int, default=200)
    parser.add_argument("--run", type=str, default="qmix")
    args = parser.parse_args()

    path_to_pymarl = os.environ.get("PYMARL_PATH",
                                    os.path.expanduser("~/pymarl/"))

    ray.init()
    ModelCatalog.register_custom_model("pa_model", ParametricActionsModel)

    register_env("starcraft", lambda cfg: SC2MultiAgentEnv(cfg))
    agent_cfg = {
        "observation_filter": "NoFilter",
        "model": {
            "custom_model": "pa_model"
        },
        "env_config": {
            "pymarl_path": path_to_pymarl
        }
    }
    if args.run.lower() == "qmix":
        def grouped_sc2(cfg):
            env = SC2MultiAgentEnv(cfg)
            grouping = {
                "group_1": list(range(len(env.n_agents))),
            }
            obs_space = Tuple([env.observation_space
                               for i in range(len(env.n_agents))])
            act_space = Tuple([env.action_space
                               for i in range(len(env.n_agents))])
            return env.with_agent_groups(
                grouping, obs_space=obs_space, act_space=act_space)

        register_env("grouped_starcraft", grouped_sc2)
        agent = QMixAgent(env="grouped_starcraft", config=agent_cfg)
    elif args.run.lower() == "pg":
        agent = PGAgent(env="starcraft", config=agent_cfg)
    elif args.run.lower() == "ppo":
        agent = PPOAgent(env="starcraft", config=agent_cfg)
    for i in range(args.num_iters):
        print(pretty_print(agent.train()))
